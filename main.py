import argparse
import datetime
import logging
import signal
import threading
import time
import tweepy
from helpers.google_helpers import update_suggested_tweet_wks, init_google_drive_clients, complete_refresh_spreadsheets, \
    update_wordmap_wks, update_stats_wks
from helpers.helpers import *
from helpers.mongo_helpers import *
from helpers.post_helper import post_tweet
from helpers.twitter_helpers import init_twitter_client, get_user_recent_tweets
from constants import *


# Option parsing
parser = argparse.ArgumentParser(SCRIPT_DESCRIPTION, epilog=EPILOG)
parser.add_argument('--refresh-db', action='store_true', default=REFRESH_DB_DISABLED,
                    required=False, help=REFRESH_DB_DESC, dest='refresh_dbs')

parser.add_argument('--refresh-wks', action='store_true', default=REFRESH_WKS_DISABLED,
                    required=False, help=REFRESH_WKS_DESC, dest='refresh_wks')

parser.add_argument('--refresh-all', action='store_true', default=REFRESH_ALL_DISABLED,
                    required=False, help=REFRESH_ALL_DESC, dest='refresh_all')

parser.add_argument('--interval', '--interval-minutes', type=int, default=DEFAULT_INTERVAL_MINUTES,
                    required=False, help=INTERVAL_DESC, dest='interval_minutes')

parser.add_argument('--debug-logs', action='store_true', default=DEBUG_LOGS_DISABLED,
                    required=False, help=DEBUG_LOG_DESC, dest='show_debug_logs')

parser.add_argument('--prod', action='store_true', default=PROD_DISABLED,
                    required=False, help=USE_PROD_DESC, dest='use_prod')

args = parser.parse_args()

db = init_mongo_client(use_prod=args.use_prod)
tweet_db = db[DB_TWEET_COLLECTION_NAME]
seen_tweet_db = db[DB_SEEN_COLLECTION_NAME]
authors_db = db[DB_AUTHORS_COLLECTION_NAME]

session_num_added = 0
exit_threads = threading.Event()


def print_bar():
    print(f'\n {"=" * 50}\n')


def quit_threads(signo, _frame):
    print("Interrupted by %d, shutting down" % signo)
    exit_threads.set()


def post_queue_listener(name, interval_minutes: int = POST_INTERVAL_MINUTES):
    logging.info(f"POST_Q_LISTENER:\t{name}")
    num_runs = 1
    SHOW_OUTPUT = args.show_debug_logs
    USE_PROD = args.use_prod
    while not exit_threads.is_set():
        logging.info(f"Thread {name}: run #{num_runs}")
        #  Get first tweet from Q
        post_q = init_mongo_client(use_prod=args.use_prod)[DB_POST_Q_COLLECTION_NAME]
        # first, check if empty
        num_docs = post_q.count_documents(filter={"posted": False})
        logging.info(f"Post Q len: {num_docs}")
        if num_docs > 0:
            #  Get earliest in Q
            earliest_res = post_q.find({"posted": False}).sort("_id", 1)
            if SHOW_OUTPUT:
                logging.info(f'\nEarliest Res: {earliest_res}')
            earliest = earliest_res.next()
            if SHOW_OUTPUT:
                logging.info(earliest["modified_text"])
            logging.info(f"Earliest ID: {earliest.get('_id')}")
            post_res, reply_res, insert_res = post_tweet(earliest, show_output=SHOW_OUTPUT, use_prod=USE_PROD)
            logging.info(f"Tweet Posted: {earliest}")
            logging.info(f"post_res: {post_res}")
            logging.info(f"reply_res: {reply_res}")
            logging.info(f"insert_res: {insert_res.acknowledged}")
            #     remove from post Q
            delete_res = post_q.delete_one({"tweet_id": earliest["tweet_id"]})
            if SHOW_OUTPUT:
                logging.info(delete_res.raw_result)
        else:
            logging.info("Q is empty...")
        next_post_run_start = datetime.datetime.now() + datetime.timedelta(minutes=POST_INTERVAL_MINUTES)
        logging.info(f'The next POST run will begin in {POST_INTERVAL_MINUTES} minutes (at approx:\t{str(next_post_run_start)})\n')
        exit_threads.wait(SECONDS_PER_MINUTE * POST_INTERVAL_MINUTES)
        num_runs += 1
    logging.info("Thread %s: finished", name)


def run_tweet_parser(time_interval_seconds: float = minutes_to_seconds()):
    print('\nStarting tweet parser...')
    print('Setting up services...', end=' ')
    USE_PROD = args.use_prod

    # Refresh mongo connections
    db = init_mongo_client(use_prod=USE_PROD)
    tweet_db = db[DB_TWEET_COLLECTION_NAME]
    seen_tweet_db = db[DB_SEEN_COLLECTION_NAME]
    authors_db = db[DB_AUTHORS_COLLECTION_NAME]

    # Set up Google Drive / Sheets
    google_clients = init_google_drive_clients()
    map_wks = google_clients[WORDMAP_WORKSHEET_NUM]
    suggest_wks = google_clients[SUGGESTION_WORKSHEET_NUM]
    stats_wks = google_clients[STATS_WORKSHEET_NUM]

    # Set up Twitter Client
    twitter_client = init_twitter_client()
    print('done.')

    run_number = 1
    global session_num_added
    session_start_time = datetime.datetime.now()
    num_minutes = seconds_to_minutes(time_interval_seconds)
    num_hours = seconds_to_hours(time_interval_seconds)
    print(f'Ready. Parser will run every {int(num_minutes)} minutes (approx. every {num_hours} hrs)')
    print_bar()
    while not exit_threads.is_set():
        # refresh connections
        if run_number > 1:
            # Refresh MongoDB connections
            db = init_mongo_client(use_prod=USE_PROD)
            tweet_db = db[DB_TWEET_COLLECTION_NAME]
            seen_tweet_db = db[DB_SEEN_COLLECTION_NAME]
            authors_db = db[DB_AUTHORS_COLLECTION_NAME]
            # Set up Google Drive / Sheets
            google_clients = init_google_drive_clients()
            map_wks = google_clients[WORDMAP_WORKSHEET_NUM]
            suggest_wks = google_clients[SUGGESTION_WORKSHEET_NUM]
            stats_wks = google_clients[STATS_WORKSHEET_NUM]
            # Set up Twitter Client
            twitter_client = init_twitter_client()

        num_added_this_run = 0
        run_start_time = datetime.datetime.now()
        print(f'RUN #{run_number}:\nStart time: {str(run_start_time)}')

        # retrieve authors each time to allow for authors being added mid-run
        known_authors_res = get_all_known_authors(author_db=authors_db, use_prod=USE_PROD)
        targets = list(map(lambda x: get_parsed_author_obj(x), known_authors_res))

        for target in targets:
            print(f'Updates from @{target.username}:')
            most_recent_tweet_id = get_most_recent_seen_tweet_id(author_id=target.author_id, use_prod=USE_PROD)
            if args.show_debug_logs:
                print(f'\tMost recent tweet ID from @{target.username}: {most_recent_tweet_id}')

            # Get data for Twitter User
            # TODO: refactor this to use known_authors_res.data
            target_user = twitter_client.get_user(username=str(target.username))

            # Get recent tweets from this user
            print(f'\tRetrieving new tweets from @{target.username}...', end=' ')
            raw_tweet_objs = get_user_recent_tweets(
                twitter_client=twitter_client,
                target=target_user.data,
                most_recent_id=most_recent_tweet_id
            )
            print('done.')
            print(f'\tRetrieved {len(raw_tweet_objs)} new Tweets from @{target.username}.')

            if len(raw_tweet_objs) > 0:
                tweet_objs = list(map(lambda x: get_parsed_tweet_obj(x, author=target), raw_tweet_objs))
                num_have_been_seen = count_num_in_seen_db(
                    seen_db=seen_tweet_db,
                    parsed_tweets=tweet_objs
                )
                print(f'\t{num_have_been_seen} of these '
                      f'{"has" if num_have_been_seen == 1 else "have"} been seen before.')
                if num_have_been_seen == len(tweet_objs):
                    print('\tAll of these have been seen before!\nSkipping...')
                else:
                    # Filter out tweets that have been seen before
                    unseen_tweets = list(
                        filter(lambda x: not tweet_in_seen_mongodb(
                            seen_db=seen_tweet_db,
                            parsed_tweet=x
                        ), tweet_objs)
                    )

                    print(f'\t{len(unseen_tweets)} of these are unseen.')

                    eligible_tweets = list(filter(tweet_is_eligible, unseen_tweets))
                    print(f'\t{len(eligible_tweets)} new Tweets by @{target.username} are eligible to be added to the '
                          f'\"{DB_TWEET_COLLECTION_NAME}\" DB.')
                    num_inserted = 0
                    if len(eligible_tweets) > 0:
                        print(f'\tInserting {len(eligible_tweets)} into \"{DB_SEEN_COLLECTION_NAME}\" DB...', end=' ')

                        insert_res = insert_parsed_tweets_to_mongodb(
                            tweet_db=tweet_db,
                            parsed_tweets=eligible_tweets
                        )

                        num_skip = insert_res["num_skipped"]
                        num_inserted = insert_res["num_inserted"]
                        print(f'done.{f" ({num_inserted} inserted, {num_skip} skipped)" if num_skip > 0 else ""}')

                    print('\tMarking all tweets as seen...', end=' ')
                    mark_seen_res = insert_many_tweets_to_seen_db(
                        seen_db=seen_tweet_db,
                        parsed_tweets=tweet_objs
                    )
                    print('done.')
                    print(f'\t{len(tweet_objs)} Tweets by @{target.username} were marked as seen.')
                    num_added_this_run += num_inserted
            else:
                print(f'\tNo new tweets by @{target.username}, we\'ll check again later...')

        print(f'This concludes run #{run_number}.')
        if num_added_this_run > 0:
            worksheet_update_num = update_suggested_tweet_wks(
                worksheet=suggest_wks,
                partial_update=True,
                show_output=args.show_debug_logs,
                use_prod=USE_PROD
            )
            update_wordmap_wks(
                worksheet=map_wks,
                show_output=args.show_debug_logs,
                use_prod=USE_PROD
            )
            update_stats_wks(worksheet=stats_wks, use_prod=USE_PROD)
            print(f'\t{worksheet_update_num} changes in worksheet')
        session_num_added += num_added_this_run

        print(f'\nTweets added this run:\t\t{num_added_this_run}')
        print(f'Total Tweets added this session:\t{session_num_added}')
        next_run_start = datetime.datetime.now() + datetime.timedelta(minutes=num_minutes)
        print(f'The next run will begin in {int(num_minutes)} minutes (at approx:\t{str(next_run_start)})')
        print_bar()
        run_number += 1
        exit_threads.wait(time_interval_seconds)
        # time.sleep(time_interval_seconds)


if __name__ == '__main__':
    try:
        print(args)
        run_interval = minutes_to_seconds(args.interval_minutes)
        show_debug_logs = args.show_debug_logs is DEBUG_LOGS_ENABLED
        if args.refresh_dbs or args.refresh_all:
            print('Refreshing Mongo database of parsed Tweets...')
            mongo_start_time = datetime.datetime.now()
            revisit_seen_tweets(show_output=args.show_debug_logs, use_prod=args.use_prod)
            mongo_end_time = datetime.datetime.now()
            mongo_elapsed_time = mongo_end_time - mongo_start_time
            print(f'Done ({mongo_elapsed_time}).')
        if args.refresh_wks or args.refresh_all:
            print('Refreshing Google Spreadsheets...')
            google_start_time = datetime.datetime.now()
            # FIXME: see FIXME on google_helpers.update_suggested_tweet_wks()
            # complete_refresh_spreadsheets(show_output=args.show_debug_logs, use_prod=args.use_prod)
            google_end_time = datetime.datetime.now()
            google_elapsed_time = google_end_time - google_start_time
            print(f'Done ({google_elapsed_time}).')

        # setup logging & threads
        info_log_format = "%(asctime)s: %(message)s"
        logging.basicConfig(format=info_log_format, level=logging.INFO, datefmt="%H:%M:%S")
        logging.info("Main    : before creating thread")
        for sig in ('TERM', 'HUP', 'INT'):
            signal.signal(getattr(signal, 'SIG' + sig), quit_threads)
        q_listener = threading.Thread(target=post_queue_listener, args=("post_queue",))
        # parser = threading.Thread(target=run_tweet_parser, args=(run_interval,))
        q_listener.start()
        # parser.start()
        run_tweet_parser(time_interval_seconds=run_interval)
        q_listener.join()

    except KeyboardInterrupt:
        logging.info("Except Main    : all done")
        print('\n\nInterrupted')
    finally:
        print(f'Total Tweets added this session:\t{session_num_added}')

