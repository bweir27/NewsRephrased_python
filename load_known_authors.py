import pprint

from helpers.helpers import get_parsed_author_obj
from helpers.mongo_helpers import *
from constants import *

# Init mongo
db = init_mongo_client()
seen_db = db[DB_SEEN_COLLECTION_NAME]
tweets_db = db[DB_TWEET_COLLECTION_NAME]
# seen_db = db["copy_of_seen_tweets"]
# tweets_db = db["copy_of_tweets_3"]
authors_db = db[DB_AUTHORS_COLLECTION_NAME]


def get_author_by_username(username):
    author_db_res = authors_db.find_one({"username": username})
    if author_db_res is None:
        raise Exception('Author not found')
    return author_db_res


def aggregate_seen_author_usernames():
    pipeline = [
        {
            "$match": {
                "$and": [
                    {"tweet_author": {"$ne": None}},
                    {"tweet_author": {"$type": "string"}}
                ]
            }
        },
        {
            "$group": {
                "_id": "$tweet_author",
                "num_records": {"$sum": 1}
            }
        }
    ]
    seen_authors = seen_db.aggregate(pipeline=pipeline)
    results = list()
    found_users = list()
    for r in seen_authors:
        results.append({"username": r["_id"], "count": r["num_records"]})
        found_users.append(r["_id"])
    # for x in results:
    #
    # found_users = list(map(lambda x: x["_id"], seen_authors))
    # for a in seen_authors:
    #     print(a)
    print(found_users)
    return found_users


def update_seen_db(author_username):
    author_from_db = get_author_by_username(username=author_username)
    author = get_parsed_author_obj(author_from_db)
    filter_obj = {
        "$or": [
            {
                "$and": [
                    {"author": {"$ne": None}},
                    {"author.username": {"$ne": None}},
                    {"author.username": author_username},
                ]
            },
            {
                "$and": [
                    {"tweet_author": {"$ne": None}},
                    {"tweet_author": {"$type": "string"}},
                    {"tweet_author": author_username}
                ]
            }
        ]
    }
    update_obj = {
        "$set": {
            "author": author.as_json()
        },
        "$unset": {
            "tweet_author": 1,
        }
    }

    seen_update_res = seen_db.update_many(
        filter=filter_obj,
        update=update_obj,
        upsert=True,
        array_filters=None
    )
    print('\nseen_update_res:')
    pprint.pprint(seen_update_res.raw_result)
#     Update seen_authors db
#     authors_filter_obj = {
#         "$and": [
#             {"tweet_author": {"$ne": None}},
#             {"tweet_author": {"$type": "string"}},
#             {"tweet_author": author_username}
#         ]
#     }
#     authors_update_obj = {
#         "$set": {
#             "author_id": map_obj[author_username] or "INVALID",
#             "author_username": author_username,
#         }
#     }
#
#     authors_update_res = authors_db.update_many(
#         filter=authors_filter_obj,
#         update=authors_update_obj,
#         upsert=True,
#         array_filters=None
#     )
#     print('\nauthors_update_res:')
#     pprint.pprint(seen_update_res.raw_result)


def update_tweet_db(author_username):
    author_from_db = get_author_by_username(username=author_username)
    author = get_parsed_author_obj(author_from_db)
    filter_obj = {
        "$or": [
            {
                "$and": [
                    {"author": {"$ne": None}},
                    {"author.username": {"$ne": None}},
                    {"author.username": author_username},
                ]
            },
            {
                "$and": [
                    {"tweet_author": {"$ne": None}},
                    {"tweet_author": {"$type": "string"}},
                    {"tweet_author": author_username}
                ]
            }
        ]
    }
    update_obj = {
        "$set": {
            "author": author.as_json()
        },
        "$unset": {
            "tweet_author": 1,
        }
    }
    tweet_update_res = tweets_db.update_many(
        filter=filter_obj,
        update=update_obj,
        upsert=True,
        array_filters=None
    )
    print('\nseen_update_res:')
    pprint.pprint(tweet_update_res.raw_result)


if __name__ == '__main__':
    # found_usernames = aggregate_seen_author_usernames()
    known_author_res = get_all_known_authors(author_db=authors_db)
    targets = list(map(lambda x: get_parsed_author_obj(x), known_author_res))
    print(f'targets: ')
    for t in targets:
        print(t)
    # update_seen_db("CNN")
    # update_tweet_db("CNN")
    # for u in found_usernames:
    #     print(f'Updating \"{u}\" authors')
    #     update_seen_db(str(u))
    #     update_tweet_db(str(u))

