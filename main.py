import csv
import datetime
import pprint
import time
from helpers.google_helpers import update_suggested_tweet_wks, init_google_drive_clients
from helpers.helpers import *
from helpers.mongo_helpers import *
from helpers.twitter_helpers import init_twitter_client

# TODO: receive and interpret input args
SOURCE_FROM_MONGO = True
INSERT_TO_SPREADSHEET = False
INSERT_TO_MONGO = True
MIN_INTERVAL_SECONDS = 100

db = init_mongo_client()
tweet_db = db[DB_TWEET_COLLECTION_NAME]
seen_tweet_db = db[DB_SEEN_COLLECTION_NAME]
authors_db = db[DB_AUTHORS_COLLECTION_NAME]

total_num_added = 0


def get_tweets_from_csv(file_name):
    foundTweets = []
    with open(file_name, encoding='utf-8') as csvf:
        csvReader = csv.DictReader(csvf)
        for row in csvReader:
            thisTweet = ParsedTweet(
                author_id=row["tweet_author"],
                tweet_id=row["tweet_author"],
                num_replacements=row["tweet_author"],
                original_text=row["original_text"],
                modified_text=row["modified_text"],
                tweet_url=row["tweet_url"],
                created_at=row["created_at"],
                mapped_keys=row["mapped_keys"]
            )
            foundTweets.append(thisTweet)
    pprint.pprint(foundTweets)
    return foundTweets


def get_tweets_from_mongo(limit=50):
    if limit < 1:
        raise Exception(f'Invalid limit value: {limit}')
    known_tweets = tweet_db.find({}).sort("tweet_id", 1).limit(limit)
    return list(known_tweets)


def get_known_tweet_ids():
    known_tweets = []
    if SOURCE_FROM_MONGO:
        known_tweets = get_tweets_from_mongo()
    else:
        known_tweets = get_tweets_from_csv(SEED_DATA_CSV)
    # known_tweets = tweet_db.find({}, {"_id": 1, "tweet_id": 1, "tweet_url": 1}).sort("tweet_id", 1)
    return list(map(lambda x: str(x["tweet_id"]), known_tweets))


def get_known_authors():
    authors_res = get_all_known_authors(author_db=authors_db)
    res = list()
    for a in authors_res:
        res.append(a)
    return res


# def get_most_recent_seen_tweet_id_mongo(author_id=None):
#     find_query = {}
#     if author_id and isinstance(author_id, str):
#         find_query = {}
#     max_id = seen_tweet_db.find(find_query).sort("tweet_id", -1)[0]
#     return str(max_id["tweet_id"])


def get_most_recent_tweet_id_mongo(author_id=None):
    find_query = {}
    if author_id:
        find_query = {"author.author_id": str(author_id)}
    max_id = tweet_db.find(find_query).sort("tweet_id", -1)[0]
    return str(max_id["tweet_id"])


# def filter_seen_tweets(max_id, tweet_data):
#     res = list()
#     if INSERT_TO_SPREADSHEET:
#
#         # res = list(filter(lambda x: filter_seen_tweets(worksheet=suggest_wks, tweet=x,
#         #                                                max_seen_id=max_id),
#         #                   tweet_data))
#     else:
#         res = list(filter(lambda x: filter_seen_tweets_mongo(db_coll=tweet_db, tweet=x,
#                                                              max_seen_id=max_id),
#                           tweet_data))
#     return res

def print_bar():
    print(f'\n {"=" * 50}\n')


def num_seconds_in_hours(num_hours: float = 1.0) -> float:
    num_seconds = round(num_hours * 60 * 60, 2)
    if num_seconds < MIN_INTERVAL_SECONDS:
        raise Exception(f'Must be greater than {MIN_INTERVAL_SECONDS}')
    return num_seconds


def convert_seconds_to_hours(num_seconds):
    num_hours = round((num_seconds / 60) / 60, 3)
    return num_hours


def run_tweet_parser(time_interval_seconds: float = num_seconds_in_hours()):
    print('Starting tweet parser...')
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
    print('Targets:')
    for t in targets:
        print(t)

    print('Beginning listener...')
    time.sleep(2)
    run_number = 1
    global total_num_added
    session_start_time = datetime.datetime.now()
    num_hrs = convert_seconds_to_hours(time_interval_seconds)
    print(f'Ready. Parser will run every {time_interval_seconds} seconds (approx. every {num_hrs} hrs)')
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
        start_time = datetime.datetime.now()
        print(f'RUN #{run_number}:\nStart time: {str(start_time)}')
        for target in targets:
            print(f'Updates from @{target.username}:')
            most_recent_tweet_id_mongo = get_most_recent_seen_tweet_id_mongo(author_id=target.author_id)
            print(f'\tMost recent tweet ID from @{target.username}: {most_recent_tweet_id_mongo}')
            # today = datetime.datetime.now()
            # yesterday = datetime.datetime(year=2022, month=7, day=6, hour=1)

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

            # If there are no new Tweets, data=None (as opposed to being an empty list)
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
                    eligible_tweets = list(
                        filter(
                            lambda x: tweet_is_eligible(x),
                            unseen_tweets
                        )
                    )
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
                        show_output=False
                    )
                    num_added_this_run += num_inserted
                    total_num_added += num_inserted
                    print(f'\t{worksheet_update_num} changes in worksheet')
                # TODO: Update last_seen and loop
                # print('Done!')

            else:
                print(f'\tNo new tweets by @{target.username}, we\'ll check again later...')
        print(f'This concludes the current run.')
        print(f'\nTweets added this run:\t\t{num_added_this_run}')
        print(f'Total Tweets added this session:\t{total_num_added}')
        next_start = start_time + datetime.timedelta(seconds=time_interval_seconds)
        print(f'The next run will begin at approx:\t{str(next_start)}')
        print_bar()
        run_number += 1
        time.sleep(time_interval_seconds)


if __name__ == '__main__':
    try:
        interval = num_seconds_in_hours(0.5)
        run_tweet_parser(time_interval_seconds=interval)
        # TODO: Post Tweet via twitter API
    except KeyboardInterrupt:
        print('\n\nInterrupted')
    finally:
        # total_num_added
        print(f'Total Tweets added this session:\t{total_num_added}')
        exit(0)
        # try:
        #     sys.exit(0)
        # except SystemExit:
        #     os._exit(0)
