from pymongo import MongoClient
from ParsedTweet import ParsedTweet
from TweetAuthor import TweetAuthor
from constants import *

# =============== SETUP OPERATIONS =========


def init_mongo_client():
    mongo_client = MongoClient(MONGO_HOST, MONGO_PORT)
    db = mongo_client[DB_NAME]
    return db


# =============== AUTHOR OPERATIONS =========


def author_is_known(parsed_author, author_db=None):
    if parsed_author is None or not isinstance(parsed_author, TweetAuthor):
        raise Exception('Invalid input for \"parsed_author\"')
    if author_db is None:
        author_db = init_mongo_client()[DB_AUTHORS_COLLECTION_NAME]
    return author_db.count_documents({"author_id": parsed_author.author_id}) > 0


def get_author_from_db(author_db=None, author_id=None, username=None):
    if all(a is None for a in [author_id, username]):
        raise Exception("Must provide at least one of the fields")
    if author_db is None:
        author_db = init_mongo_client()[DB_AUTHORS_COLLECTION_NAME]
    if author_id:
        field = "author_id"
        val = str(author_id)
    else:
        field = "username"
        val = str(username)
    return author_db.find_one({field: val})


def get_all_known_authors(author_db=None):
    if author_db is None:
        author_db = init_mongo_client()[DB_AUTHORS_COLLECTION_NAME]
    return author_db.find({})


def insert_author_into_db(parsed_author, author_db=None):
    if parsed_author is None or not isinstance(parsed_author, TweetAuthor):
        raise Exception('Invalid input for \"parsed_author\" '
                        f'(expected type TweetAuthor, received {type(parsed_author)}')
    if author_db is None:
        author_db = init_mongo_client()[DB_AUTHORS_COLLECTION_NAME]
    return author_db.update_one(
        filter={"author_id": str(parsed_author.author_id)},
        update={"$set": parsed_author.as_json()},
        upsert=True,
        array_filters=None
    )


# ========= SEEN Tweet Operations ===========

def get_oldest_seen_tweet_id_mongo():
    seen_tweet_db = init_mongo_client()[DB_SEEN_COLLECTION_NAME]
    min_id = seen_tweet_db.find({}).sort("tweet_id", 1).limit(1)[0]
    return str(min_id["tweet_id"])


def get_most_recent_seen_tweet_id_mongo(author_id=None):
    seen_tweet_db = init_mongo_client()[DB_SEEN_COLLECTION_NAME]
    find_query = {}
    if author_id:
        auth_id = str(author_id)
        find_query = {"author.author_id": auth_id}
    #     Ensure at least one tweet from this author has been seen before
        num_seen = seen_tweet_db.count_documents(find_query)
        if num_seen is None or num_seen <= 0:
            return get_oldest_seen_tweet_id_mongo()
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
    res = seen_db.count_documents({"tweet_id": tweet_id_str})
    return res > 0


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
        filter={"tweet_id": str(parsed_tweet.tweet_id)},
        update=update_arg,
        upsert=True,
        array_filters=None
    )
    return res


def insert_many_tweets_to_seen_db(seen_db, parsed_tweets):
    res_arr = list()
    for t in parsed_tweets:
        if not isinstance(t, ParsedTweet):
            raise Exception(f'Invalid Tweet Object: {t}')
        r = insert_tweet_to_seen_db(seen_db=seen_db, parsed_tweet=t)
        res_arr.append(r)
    return res_arr


# ========= SEEN Tweet Operations ===========


def get_most_recent_tweet_id_from_mongo(mongo_collection, author_id=None):
    find_query = {}
    if author_id:
        find_query = {"author.author_id": str(author_id)}
    latest_id = mongo_collection.find(find_query).sort("tweet_id", -1).limit(1)[0]  # [("tweet_id", pymongo.DESCENDING)])
    return str(latest_id["tweet_id"])


def tweet_in_mongo(db_coll, tweet):
    if "tweet_id" not in tweet:
        if "id" not in tweet:
            raise Exception('Invalid Tweet')
        id_val = str(tweet["id"])
    else:
        id_val = str(tweet["tweet_id"])
    return db_coll.count_documents({"tweet_id": id_val}) > 0


# def tweet_has_been_seen_mongo(db_coll, tweet, max_seen_id="1"):
#     if "tweet_id" not in tweet:
#         if "id" not in tweet:
#             raise Exception('Invalid Tweet')
#         id_val = str(tweet["id"])
#     else:
#         id_val = str(tweet["tweet_id"])
#     last_seen_tweet_id = max_seen_id
#     if max_seen_id <= "1":
#         last_seen_tweet_id = get_most_recent_tweet_id_from_mongo(db_coll)
#     return id_val < last_seen_tweet_id


# def unseen_tweet_mongo(db_coll, tweet, max_seen_id="0"):
#     return not tweet_has_been_seen_mongo(db_coll=db_coll, tweet=tweet, max_seen_id=max_seen_id)


# def filter_seen_tweets_mongo(db_coll, tweet, max_seen_id="0"):
#     return unseen_tweet_mongo(db_coll=db_coll, tweet=tweet, max_seen_id=max_seen_id)


def insert_parsed_tweet_to_mongodb(tweet_db, parsed_tweet):
    if not isinstance(parsed_tweet, ParsedTweet):
        raise Exception(f'Invalid Tweet Object: {parsed_tweet}')
    res = tweet_db.insert_one(parsed_tweet.as_json())
    return res


def insert_parsed_tweets_to_mongodb(tweet_db, parsed_tweets, filter_seen=True):
    res_list = list()
    num_skipped = 0
    for t in parsed_tweets:
        if not isinstance(t, ParsedTweet):
            raise Exception(f'Invalid Tweet Object: {t}')
        if t.num_replacements < 1:
            num_skipped += 1
            continue
        update_arg = {
            "$set": t.as_json()
        }
        r = tweet_db.update_one(filter={"tweet_id": t.tweet_id},
                                update=update_arg,
                                upsert=True,
                                array_filters=None)
        res_list.append(r)
    return {
        "num_skipped": num_skipped,
        "res_list": res_list
    }

# def update_tweet_mongo_db(db, tweetData, filter_unseen=True, last_seen_id=-1):
#     unseen_tweets = tweetData
#     if filter_unseen:
#         if float(last_seen_id) < 0:
#             last_seen_id = str(get_most_recent_tweet_id_from_mongo(db))
#         # unseen_tweets = list(
#         #     filter(lambda x: filter_seen_tweets(worksheet=worksheet, tweet=x, max_seen_id=last_seen_id), tweetData)
#         # )
#     to_add = []
#     for t in tweetData:
#         if filter_unseen:
#             if str(t["tweet_id"]) > str(last_seen_id):
#                 this_tweet = ParsedTweet(
#                     author_id=t["tweet_author"],
#                     tweet_id=t["tweet_id"],
#                     num_replacements=t["num_replacements"],
#                     original_text=t["original_text"],
#                     modified_text=t["modified_text"],
#                     tweet_url=t["tweet_url"],
#                     created_at=str(t["created_at"]),
#                     mapped_keys=t["mapped_keys"]
#                 )
#                 to_add.append(this_tweet.as_json())
#         else:
#             curr_tweet = ParsedTweet(
#                 author_id=t["tweet_author"],
#                 tweet_id=float(t["tweet_id"]),
#                 num_replacements=t["num_replacements"],
#                 original_text=t["original_text"],
#                 modified_text=t["modified_text"],
#                 tweet_url=t["tweet_url"],
#                 created_at=str(t["created_at"]),
#                 mapped_keys=t["mapped_keys"]
#             )
#             to_add.append(curr_tweet.as_json())
#     num_added = len(to_add)
#     db.insert_many(to_add)
#     return num_added
