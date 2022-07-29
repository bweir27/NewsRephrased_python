import pprint
import re
import time

from ParsedTweet import ParsedTweet
from TweetAuthor import TweetAuthor
from constants import *
from helpers.mongo_helpers import init_mongo_client, get_all_known_authors, insert_parsed_tweets_to_mongodb, \
    insert_many_tweets_to_seen_db, get_total_num_replacements
from helpers.twitter_helpers import init_twitter_client
from replacement_filter import apply_replacement_filter


def tweet_is_eligible(parsed_tweet: ParsedTweet) -> bool:
    if not isinstance(parsed_tweet, ParsedTweet):
        raise Exception(f'Invalid Tweet Object: {parsed_tweet}')
    return parsed_tweet.num_replacements > 0


def sort_by_tweet_id(tweet) -> str:
    return str(tweet["tweet_id"])


def get_date_for_tweet_by_id(t_id, twitter_client):
    t = twitter_client.get_tweet(id=t_id, tweet_fields=["id", "text", "created_at"], expansions=["author_id"])
    return t.data["created_at"]


def extract_tweet_id_from_url(url: str) -> str:
    str_url = str(url)
    if BASE_URL not in str_url:
        raise Exception(f'Invalid URL, must start with "{BASE_URL}"')
    id_extract_re = re.compile(f'{BASE_URL}[A-Za-z0-9]*\/status\/', re.IGNORECASE)
    tweet_id = re.sub(id_extract_re, '', str_url)
    return str(tweet_id)


def get_parsed_author_obj(author_from_db) -> TweetAuthor:
    return TweetAuthor(
        name=author_from_db["name"],
        username=author_from_db["username"],
        author_id=author_from_db["author_id"]
    )


def get_parsed_tweet_obj(tweet, author: TweetAuthor) -> ParsedTweet:
    if not isinstance(author, TweetAuthor):
        raise Exception('Invalid type for \"author\"'
                        f' (expected TweetAuthor, provided {type(author)})')
    filterInfo = apply_replacement_filter(tweet["text"])
    res = ParsedTweet(
        author=author,
        tweet_id=str(tweet["id"]),
        num_replacements=filterInfo["num_replacements"],
        original_text=filterInfo["original_text"],
        modified_text=filterInfo["modified_text"],
        tweet_url=f'{BASE_URL}{author.username}/status/{tweet["id"]}',
        created_at=tweet["created_at"],
        repl_freq_map=filterInfo["replaced_key_freq"]
    )
    return res


def drop_unposted_tweets(use_prod: bool = False):
    db = init_mongo_client(use_prod=use_prod)
    tweet_db = db[DB_TWEET_COLLECTION_NAME]
    posted_res = tweet_db.find({"posted": True}).sort("tweet_id", 1)
    posted_ids = list()
    posted_text = set()
    for t in posted_res:
        posted_ids.append(t["tweet_id"])
        posted_text.add(t["modified_text"])
    delete_res = tweet_db.delete_many(filter={"$and": [{"posted": False}, {"tweet_id": {"$nin": posted_ids}}]})
    num_remaining = tweet_db.count_documents({})


def drop_eligible_duplicates(use_prod: bool = False):
    db = init_mongo_client(use_prod=use_prod)
    tweet_db = db[DB_TWEET_COLLECTION_NAME]
    seen_db = db[DB_SEEN_COLLECTION_NAME]
    # get all posted tweets
    posted_res = tweet_db.find({"posted": True}).sort("tweet_id", 1)
    posted_ids = set()
    posted_text = set()
    for t in posted_res:
        posted_ids.add(t["tweet_id"])
        posted_text.add(t["modified_text"])
    # get all unposted tweets
    unposted_res = tweet_db.find().sort("tweet_id", 1)

    seen = set()
    unposted_mod_text = set()
    u_ids = list()
    for t in unposted_res:
        mod_txt = t["modified_text"]
        seen.add(t["tweet_id"])
        if mod_txt not in unposted_mod_text and mod_txt not in posted_text:
            unposted_mod_text.add(mod_txt)
            u_ids.append(t["tweet_id"])
    delete_res = tweet_db.delete_many(filter={"$and": [{"posted": False}, {"tweet_id": {"$nin": u_ids}}]})
    num_remaining = tweet_db.count_documents({})


def revisit_seen_tweets(show_output=False, use_prod: bool = False):
    # refresh Mongo connections
    db = init_mongo_client(use_prod=use_prod)
    tweet_db = db[DB_TWEET_COLLECTION_NAME]
    seen_db = db[DB_SEEN_COLLECTION_NAME]
    drop_unposted_tweets()
    num_start = tweet_db.count_documents({})
    if show_output:
        print(f"startNum: {num_start}")
    # refresh Twitter client
    twitter_client = init_twitter_client()
    all_seen_tweet_docs = seen_db.find({}).sort("tweet_id", -1)

    # get list of IDs for all the "seen" tweets
    to_visit_ids = list()
    for t in all_seen_tweet_docs:
        to_visit_ids.append(str(t["tweet_id"]))
    # have to split list into segments of 100 because twitter API only allows 100 at a time
    split_size = 100
    a_splitted = [to_visit_ids[x:x + split_size] for x in range(0, len(to_visit_ids), split_size)]

    known_authors_res = get_all_known_authors()
    authors = list(map(lambda x: get_parsed_author_obj(x), known_authors_res))
    auth_dict = {}
    total_num_skipped = 0
    total_num_retrieved = 0
    for a in authors:
        auth_dict[str(a.author_id)] = a

    for segment in a_splitted:
        retrieved_tweets = twitter_client.get_tweets(ids=segment,
                                                     expansions="author_id",
                                                     tweet_fields=["id", "text", "created_at"],
                                                     user_fields=["username"]
                                                     )
        total_num_retrieved += len(retrieved_tweets.data)
        if show_output:
            print(f'{len(retrieved_tweets.data)} Tweets retrieved.')
        tweet_objs = list(map(lambda x: get_parsed_tweet_obj(x, auth_dict[str(x["author_id"])]), retrieved_tweets.data))
        if show_output:
            print('Inserting...')
        seen_res = insert_many_tweets_to_seen_db(seen_db=seen_db, parsed_tweets=tweet_objs)
        insert_res = insert_parsed_tweets_to_mongodb(tweet_db=tweet_db, parsed_tweets=tweet_objs)
        total_num_skipped += insert_res["num_skipped"]
        if show_output:
            print(f'{insert_res["num_skipped"]} skipped.')
    if show_output:
        print('Done.')
        print(f"startNum: {num_start}")
        print(f"Total retrieved: {total_num_retrieved}")
        print(f"Total skipped: {total_num_skipped}")
        print(f"Net: {total_num_retrieved - total_num_skipped}")
    drop_eligible_duplicates()

# =============== UNIT CONVERSIONS  =========


def hours_to_seconds(num_hours: float = 1.0) -> float:
    num_seconds = round(num_hours * SECONDS_PER_HOUR, 2)
    if num_seconds < MIN_INTERVAL_SECONDS:
        raise Exception(f'Interval must be longer than {MIN_INTERVAL_SECONDS}')
    return num_seconds


def minutes_to_seconds(num_minutes: int = DEFAULT_INTERVAL_MINUTES) -> float:
    num_seconds = round(num_minutes * SECONDS_PER_MINUTE, 2)
    if num_seconds < MIN_INTERVAL_SECONDS:
        raise Exception(f'Interval must be longer than {MIN_INTERVAL_SECONDS}')
    return num_seconds


def seconds_to_minutes(num_seconds: float) -> float:
    num_minutes = round((num_seconds / SECONDS_PER_MINUTE), 3)
    return num_minutes


def seconds_to_hours(num_seconds):
    num_hours = round(num_seconds / SECONDS_PER_HOUR, 3)
    return num_hours

