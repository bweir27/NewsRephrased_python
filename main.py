import argparse
import datetime
import time

from helpers.google_helpers import update_suggested_tweet_wks, init_google_drive_clients, complete_refresh_spreadsheets, \
    update_wordmap_wks
from helpers.helpers import *
from helpers.mongo_helpers import *
from helpers.twitter_helpers import init_twitter_client
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

# TODO: add DEV_ENV arg

args = parser.parse_args()

db = init_mongo_client()
tweet_db = db[DB_TWEET_COLLECTION_NAME]
seen_tweet_db = db[DB_SEEN_COLLECTION_NAME]
authors_db = db[DB_AUTHORS_COLLECTION_NAME]

session_num_added = 0


def print_bar():
    print(f'\n {"=" * 50}\n')


def run_tweet_parser(time_interval_seconds: float = minutes_to_seconds()):
    print('\nStarting tweet parser...')
    print('Setting up services...', end=' ')

    # Refresh mongo connections
    db = init_mongo_client()
    tweet_db = db[DB_TWEET_COLLECTION_NAME]
    seen_tweet_db = db[DB_SEEN_COLLECTION_NAME]
    authors_db = db[DB_AUTHORS_COLLECTION_NAME]

    # Set up Google Drive / Sheets
    google_clients = init_google_drive_clients()
    map_wks = google_clients[WORDMAP_WORKSHEET_NUM]
    suggest_wks = google_clients[SUGGESTION_WORKSHEET_NUM]

    # Set up Twitter
    twitter_client = init_twitter_client()
    print('done.')

    # TODO: Retrieve all known targets
    print('Retrieving targets...', end='')
    known_authors_res = get_all_known_authors(author_db=authors_db)
    targets = list(map(lambda x: get_parsed_author_obj(x), known_authors_res))
    print('done.')

    if args.show_debug_logs:
        print('Targets:')
        for t in targets:
            print(t)

    time.sleep(2)
    run_number = 1
    global session_num_added
    session_start_time = datetime.datetime.now()
    num_minutes = seconds_to_minutes(time_interval_seconds)
    num_hours = seconds_to_hours(time_interval_seconds)
    print(f'Ready. Parser will run every {int(num_minutes)} minutes (approx. every {num_hours} hrs)')
    print_bar()
    while True:
        # refresh connections
        if run_number > 1:
            # Refresh mongo connections
            db = init_mongo_client()
            tweet_db = db[DB_TWEET_COLLECTION_NAME]
            seen_tweet_db = db[DB_SEEN_COLLECTION_NAME]
            authors_db = db[DB_AUTHORS_COLLECTION_NAME]
            # Set up Google Drive / Sheets
            google_clients = init_google_drive_clients()
            map_wks = google_clients[WORDMAP_WORKSHEET_NUM]
            suggest_wks = google_clients[SUGGESTION_WORKSHEET_NUM]
            # Set up Twitter
            twitter_client = init_twitter_client()

        num_added_this_run = 0
        run_start_time = datetime.datetime.now()
        print(f'RUN #{run_number}:\nStart time: {str(run_start_time)}')
        for target in targets:
            print(f'Updates from @{target.username}:')
            most_recent_tweet_id_mongo = get_most_recent_seen_tweet_id_mongo(author_id=target.author_id)
            print(f'\tMost recent tweet ID from @{target.username}: {most_recent_tweet_id_mongo}')

            # Get User data for Twitter User
            target_user = twitter_client.get_user(username=str(target.username))

            # TODO: Query for paginated results
            # Get recent tweets from this user
            print(f'\tRetrieving new tweets from @{target.username}...', end=' ')
            target_recent_tweets = twitter_client.get_users_tweets(
                id=target_user.data.id,
                since_id=most_recent_tweet_id_mongo,
                exclude=["retweets", "replies"],
                max_results=100,
                tweet_fields=["id", "text", "created_at"],
                expansions=["author_id"],
                user_fields=["username"]
            )
            print('done.')
            result_count = target_recent_tweets.meta["result_count"]
            print(f'\tRetrieved {result_count} new Tweets from @{target.username}.')

            # If there are no new Tweets, Twitter / Tweepy API returns data=None (as opposed to an empty list)
            if target_recent_tweets.data:
                tweet_objs = list(map(lambda x: get_parsed_tweet_obj(x, author=target), target_recent_tweets.data))
                print('\tChecking how many of these have already been seen...')
                num_have_been_seen = count_num_in_seen_db(
                    seen_db=seen_tweet_db,
                    parsed_tweets=tweet_objs
                )
                print(f'\t{num_have_been_seen} of these {"has" if num_have_been_seen == 1 else "have"} been seen before.')
                if num_have_been_seen == len(tweet_objs):
                    print('\tAll of these have been seen before!\nExiting...')
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
                    print(f'\t{len(eligible_tweets)} Tweets by @{target.username} are eligible to be added to the \"tweets\" DB.')
                    print(f'\tInserting {len(eligible_tweets)} into \"tweets\" DB...', end=' ')
                    insert_res = insert_parsed_tweets_to_mongodb(
                        tweet_db=tweet_db,
                        parsed_tweets=eligible_tweets
                    )
                    num_skip = insert_res["num_skipped"]
                    num_inserted = len(insert_res["res_list"])
                    print(f'done.{f" ({num_inserted} inserted, {num_skip} skipped)" if num_skip > 0 else ""}')

                    print('\tMarking all tweets as seen...', end=' ')
                    mark_seen_res = insert_many_tweets_to_seen_db(
                        seen_db=seen_tweet_db,
                        parsed_tweets=tweet_objs
                    )
                    print('done.')
                    print(f'\t{len(tweet_objs)} Tweets by @{target.username} were marked as seen.')
                    worksheet_update_num = update_suggested_tweet_wks(
                        worksheet=suggest_wks,
                        partial_update=True,
                        show_output=args.show_debug_logs
                    )
                    update_wordmap_wks(
                        worksheet=map_wks,
                        show_output=args.show_debug_logs
                    )
                    num_added_this_run += num_inserted
                    session_num_added += num_inserted
                    print(f'\t{worksheet_update_num} changes in worksheet')
            else:
                print(f'\tNo new tweets by @{target.username}, we\'ll check again later...')
        print(f'This concludes the current run.')
        print(f'\nTweets added this run:\t\t{num_added_this_run}')
        print(f'Total Tweets added this session:\t{session_num_added}')
        next_run_start = run_start_time + datetime.timedelta(seconds=time_interval_seconds)
        print(f'The next run will begin in {int(num_minutes)} minutes (at approx:\t{str(next_run_start)})')
        print_bar()
        run_number += 1
        time.sleep(time_interval_seconds)


if __name__ == '__main__':
    try:
        print(args)
        run_interval = minutes_to_seconds(args.interval_minutes)
        # show_debug_logs = args.show_debug_logs is DEBUG_LOGS_ENABLED
        if args.refresh_dbs or args.refresh_all:
            print('Refreshing Mongo database of parsed Tweets...', end='')
            revisit_seen_tweets(show_output=args.show_debug_logs)
            print('done.')
        if args.refresh_wks or args.refresh_all:
            print('Refreshing Google Spreadsheets...', end='')
            complete_refresh_spreadsheets(show_output=args.show_debug_logs)
            print('done.')

        run_tweet_parser(time_interval_seconds=run_interval)
        # TODO: Post Tweet via twitter API
    except KeyboardInterrupt:
        print('\n\nInterrupted')
    finally:
        print(f'Total Tweets added this session:\t{session_num_added}')

