import json
import pymongo.results
from pymongo import MongoClient, database, results, response
import secrets
from ParsedTweet import ParsedTweet
from TweetAuthor import TweetAuthor
from constants import *


# =============== SETUP OPERATIONS =========
def init_mongo_client(use_prod=False) -> pymongo.database.Database:
    host = MONGO_HOST
    if use_prod:
        host = secrets.MONGO_ATLAS_CONNECTION_STRING
    mongo_client = MongoClient(host)
    return mongo_client[DB_NAME]


# =============== AUTHOR OPERATIONS =========
def author_is_known(parsed_author, author_db=None, use_prod: bool = False) -> bool:
    if parsed_author is None or not isinstance(parsed_author, TweetAuthor):
        raise Exception('Invalid input for \"parsed_author\"')
    if author_db is None:
        author_db = init_mongo_client(use_prod=use_prod)[DB_AUTHORS_COLLECTION_NAME]
    return author_db.count_documents({"author_id": parsed_author.author_id}) > 0


def get_author_from_db(author_db=None, author_id=None, username=None, use_prod: bool = False):
    if all(a is None for a in [author_id, username]):
        raise Exception("Must provide at least one of the fields")
    if author_db is None:
        author_db = init_mongo_client(use_prod=use_prod)[DB_AUTHORS_COLLECTION_NAME]
    if author_id:
        field = "author_id"
        val = str(author_id)
    else:
        field = "username"
        val = str(username)
    return author_db.find_one({field: val})


def get_all_known_authors(author_db=None, use_prod: bool = False):
    if author_db is None:
        author_db = init_mongo_client(use_prod=use_prod)[DB_AUTHORS_COLLECTION_NAME]
    return author_db.find({})


def insert_author_into_db(parsed_author: TweetAuthor, author_db=None, use_prod: bool = False) -> pymongo.results.UpdateResult:
    if parsed_author is None or not isinstance(parsed_author, TweetAuthor):
        raise Exception('Invalid input for \"parsed_author\" '
                        f'(expected type TweetAuthor, received {type(parsed_author)}')
    if author_db is None:
        author_db = init_mongo_client(use_prod=use_prod)[DB_AUTHORS_COLLECTION_NAME]
    return author_db.update_one(
        filter={"author_id": str(parsed_author.author_id)},
        update={"$set": parsed_author.as_json()},
        upsert=True,
        array_filters=None
    )


# ========= SEEN Tweet Operations ===========

def get_oldest_seen_tweet_id_mongo(use_prod: bool = False):
    seen_tweet_db = init_mongo_client(use_prod=use_prod)[DB_SEEN_COLLECTION_NAME]
    # TODO: handle empty DB
    min_id = seen_tweet_db.find({}).sort("_id", 1).limit(1)[0]
    return str(min_id["_id"])


def get_most_recent_seen_tweet_id_mongo(author_id=None, use_prod: bool = False):
    seen_tweet_db = init_mongo_client(use_prod=use_prod)[DB_SEEN_COLLECTION_NAME]
    find_query = {}
    if author_id:
        auth_id = str(author_id)
        find_query = {"author.author_id": auth_id}
        #     Ensure at least one tweet from this author has been seen before
        num_seen = seen_tweet_db.count_documents(find_query)
        if num_seen is None or num_seen <= 0:
            #     TODO: handle empty seen tweet DB
            return get_oldest_seen_tweet_id_mongo(use_prod=use_prod)
    max_id = seen_tweet_db.find(find_query).sort("tweet_id", -1)[0]
    return str(max_id["tweet_id"])


def tweet_in_seen_mongodb(seen_db, parsed_tweet=None, tweet_id=None):
    if parsed_tweet is None and tweet_id is None:
        raise Exception('Must have either \"parsed_tweet\" or \"tweet_id\"')
    if parsed_tweet is not None and not isinstance(parsed_tweet, ParsedTweet):
        raise Exception('\"parsed_tweet\" must be of type ParsedTweet')
    if parsed_tweet is not None:
        tweet_id_str = str(parsed_tweet.tweet_id)
    else:
        tweet_id_str = str(tweet_id)
    res = seen_db.count_documents({"_id": tweet_id_str})
    return res > 0


def count_seen_eligible_by_author(seen_db=None, use_prod: bool = False):
    if seen_db is None:
        seen_db = init_mongo_client(use_prod=use_prod)[DB_SEEN_COLLECTION_NAME]
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"author": {"$ne": None}},
                    {"author.author_id": {"$ne": None}}
                ]
            }
        },
        {
            "$group": {
                "_id": "$author.author_id",
                "name": {"$max": "$author.name"},           # FIXME: There has to be a better way to do this
                "username": {"$max": "$author.username"},
                "formatted": {"$max": {"$concat": ["$author.name", " (@", "$author.username", ")"]}},
                "seen": {"$sum": 1},
                "eligible": {"$sum": {"$cond": ["$valid_tweet", 1, 0]}}
            }
        },
        {
            "$sort": {"formatted": 1}   # sort alphabetically
        }
    ]
    res = seen_db.aggregate(pipeline=pipeline)
    return res


def count_seen_eligible_by_author_list(seen_db=None, use_prod: bool = False):
    if seen_db is None:
        seen_db = init_mongo_client(use_prod=use_prod)[DB_SEEN_COLLECTION_NAME]
    agg_res = count_seen_eligible_by_author(seen_db)
    return list([x for x in agg_res])


def count_total_in_seen_db(seen_db=None, use_prod: bool = False):
    if seen_db is None:
        seen_db = init_mongo_client(use_prod=use_prod)[DB_SEEN_COLLECTION_NAME]
    return seen_db.count_documents({})


def count_num_in_seen_db(seen_db, parsed_tweets):
    id_map = list(map(lambda x: str(x.tweet_id), parsed_tweets))
    find_obj = {
        "tweet_id": {"$in": id_map}
    }
    res = seen_db.count_documents(find_obj)
    return res


def insert_tweet_to_seen_db(seen_db, parsed_tweet):
    if not isinstance(parsed_tweet, ParsedTweet):
        raise Exception('Invalid Tweet Object')
    update_arg = {
        "$set": parsed_tweet.seen_tweet()
    }
    res = seen_db.update_one(
        filter={"_id": str(parsed_tweet.tweet_id)},
        update=update_arg,
        upsert=True,
        array_filters=None
    )
    return res


def insert_many_tweets_to_seen_db(seen_db, parsed_tweets):
    if len(parsed_tweets) == 0:
        return None
    find_filter = {"tweet_id": {"$in": [x.tweet_id for x in parsed_tweets]}}
    remove_res = seen_db.delete_many(filter=find_filter)
    insert_res = seen_db.insert_many(documents=[x.seen_tweet() for x in parsed_tweets])
    return insert_res


# ========= Tweet Operations ===========

def count_num_eligible_in_db(collection=None, use_prod: bool = False):
    if collection is None:
        collection = init_mongo_client(use_prod=use_prod)[DB_TWEET_COLLECTION_NAME]
    return collection.count_documents({})


def get_most_recent_tweet_id_from_mongo(mongo_collection, author_id=None) -> str:
    """
    :param mongo_collection:
    :param author_id: (optional) - the Twitter ID of the target author
    :return: the Twitter ID of the most recent tweet
    """
    find_query = {}
    if author_id:
        find_query = {"author.author_id": str(author_id)}
    latest_id = mongo_collection.find(find_query).sort("_id", -1).limit(1)[0]
    return str(latest_id["_id"])


def tweet_in_mongo(db_coll, tweet):
    if "tweet_id" not in tweet:
        if "id" not in tweet:
            raise Exception('Invalid Tweet')
        id_val = str(tweet["id"])
    else:
        id_val = str(tweet["tweet_id"])
    return db_coll.count_documents({"_id": id_val}) > 0


def insert_parsed_tweet_to_mongodb(tweet_db, parsed_tweet) -> pymongo.results.UpdateResult:
    if not isinstance(parsed_tweet, ParsedTweet):
        raise Exception(f'Invalid Tweet Object: {parsed_tweet}')
    return tweet_db.update_one(
        filter={"tweet_id": parsed_tweet.tweet_id},
        update={"$set": parsed_tweet.as_json()},
        upsert=True
    )


def insert_parsed_tweets_to_mongodb(tweet_db, parsed_tweets, filter_seen=True):
    num_skipped = 0
    to_post_ids = list()
    insert_docs = list()
    mapped_ids = [x.tweet_id for x in parsed_tweets]
    ignore_ids = set()
    already_posted = tweet_db.find(
        {
            "$and": [
                {"tweet_id": {"$in": mapped_ids}},
                {"posted": True}
            ]
        }
    )
    for doc in already_posted:
        ignore_ids.add(doc["tweet_id"])

    eligible = list()
    for x in parsed_tweets:
        if x.is_eligible:
            eligible.append(x)
        else:
            ignore_ids.add(x.tweet_id)

    for t in eligible:
        if not isinstance(t, ParsedTweet):
            raise Exception(f'Invalid Tweet Object: {t}')
        already_seen = tweet_db.count_documents({"modified_text": t.modified_text})
        if t.tweet_id in ignore_ids or already_seen > 0 or not t.is_eligible:
            num_skipped += 1
            continue
        to_post_ids.append(t.tweet_id)
        insert_docs.append(t.as_json())

    remove_res = None
    insert_res = None
    insert_count = len(insert_docs)
    if len(insert_docs) > 0:
        remove_res = tweet_db.delete_many(filter={"tweet_id": {"$in": to_post_ids}})
        insert_res = tweet_db.insert_many(documents=insert_docs)
        insert_count = len(insert_res.inserted_ids)
    return {
        "num_skipped": num_skipped,
        "num_inserted": insert_count,
        "raw_res": insert_res,
    }

    # for t in parsed_tweets:
    #     if not isinstance(t, ParsedTweet):
    #         raise Exception(f'Invalid Tweet Object: {t}')
    #     already_posted = tweet_db.count_documents({"$and": [{"_id": t.tweet_id}, {"posted": True}]})
    #     already_seen = tweet_db.count_documents({"modified_text": t.modified_text})
    #     if t.num_replacements < 1 or already_posted > 0 or already_seen > 0:
    #         num_skipped += 1
    #         continue
    #     update_arg = {
    #         "$set": t.as_json()
    #     }
    #     r = tweet_db.update_one(filter={"_id": t.tweet_id},
    #                             update=update_arg,
    #                             upsert=True,
    #                             array_filters=None)
    #     res_list.append(r)
    # return {
    #     "num_skipped": num_skipped,
    #     "res_list": res_list
    # }


def insert_posted_tweet_to_db(posted_obj: dict, posted_db=None, use_prod: bool = False):
    if posted_db is None:
        posted_db = init_mongo_client(use_prod=use_prod)[DB_POSTED_COLLECTION_NAME]
    insert_res = posted_db.update_one(
        filter={"_id": posted_obj["_id"]},
        update={"$set": posted_obj},
        upsert=True,
        array_filters=None
    )
    return insert_res


def get_mongo_tweet_by_id(tweet_id: str, tweet_db=None, use_prod: bool = False):
    if tweet_db is None:
        tweet_db = init_mongo_client(use_prod=use_prod)[DB_TWEET_COLLECTION_NAME]
    found_tweet = tweet_db.find_one({"_id": str(tweet_id)})
    return found_tweet


# ========= Stats / Other Operations ===========


def get_total_num_replacements(use_prod: bool = False):
    tweet_db = init_mongo_client(use_prod=use_prod)[DB_TWEET_COLLECTION_NAME]
    pipeline = [
        {"$group": {"_id": "num_repl", "count": {"$sum": "$num_replacements"}}},
    ]
    agg_res = tweet_db.aggregate(pipeline=pipeline).next()
    return agg_res


def get_key_freq_map(use_prod: bool = False):
    """
    Returns a frequency table containing the number of times each key in the wordmap has been used
        -   Note: unused words will not be in the freqTable
    """
    tweet_db = init_mongo_client(use_prod=use_prod)[DB_TWEET_COLLECTION_NAME]
    pipeline = [
        {"$unwind": "$mapped_key_list"},
        {"$group": {"_id": "$mapped_key_list.key", "count": {"$sum": "$mapped_key_list.freq"}}},
        {"$group": {
            "_id": None,
            "counts": {
                "$push": {
                    "k": "$_id",
                    "v": "$count"
                }
            }
        }},
        {"$replaceRoot": {
            "newRoot": {"$arrayToObject": "$counts"}
        }}
    ]
    counts = tweet_db.aggregate(pipeline=pipeline).next()
    return counts


def get_num_tweets_posted(use_prod: bool = False):
    tweet_db = init_mongo_client(use_prod=use_prod)[DB_TWEET_COLLECTION_NAME]
    return tweet_db.count_documents({"posted": True})


# def get_num_tweets_posted_for_author(use_prod: bool = False):
#     tweet_db = init_mongo_client(use_prod=use_prod)[DB_TWEET_COLLECTION_NAME]
#     return tweet_db.count_documents({})


def get_num_tweets_posted_per_author(use_prod: bool = False):
    tweet_db = init_mongo_client(use_prod=use_prod)[DB_TWEET_COLLECTION_NAME]
    pipeline = [
        {
            "$match": {"posted": True}
        },
        {
            "$group": {"_id": "$author.formatted", "count": {"$sum": 1}}
        },
        {
            "$group": {
                "_id": None,
                "counts": {
                    "$push": {
                        "k": "$_id",
                        "v": "$count"
                    }
                }
            }
        },
        {
            "$replaceRoot": {
                "newRoot": {"$arrayToObject": "$counts"}
            }
        }
    ]
    counts = tweet_db.aggregate(pipeline=pipeline).next()
    return counts

