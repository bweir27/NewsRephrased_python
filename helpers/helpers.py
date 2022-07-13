import re
from ParsedTweet import ParsedTweet
from TweetAuthor import TweetAuthor
from constants import *
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


# def get_most_recent_tweet_id_from_mongo(mongo_collection):
#     latest_id = mongo_collection.find().sort("tweet_id", -1).limit(1)[0]    #   [("tweet_id", pymongo.DESCENDING)])
#     return str(latest_id["tweet_id"])


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
        mapped_keys=filterInfo["replaced_keys"]
    )
    return res


# def parse_tweet(username, tweet):
#     source = username
#     tweet_info = tweet["id"]
#     filterInfo = apply_replacement_filter(tweet["text"])
#     return {
#         "tweet_author": username,
#         "tweet_id": str(tweet["id"]),
#         "num_replacements": filterInfo["num_replacements"],
#         "original_text": filterInfo["original_text"],
#         "modified_text": filterInfo["modified_text"],
#         "tweet_url": f'{BASE_URL}{username}/status/{tweet["id"]}',
#         "created_at": tweet["created_at"],
#         "mapped_keys": filterInfo["replaced_keys"]
#     }


# def map_parse_tweet(username, tweet):
#     return parse_tweet(username, tweet)

#
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
#                     tweet_author=t["tweet_author"],
#                     tweet_id=t["tweet_id"],
#                     num_replacements=t["num_replacements"],
#                     original_text=t["original_text"],
#                     modified_text=t["modified_text"],
#                     tweet_url=t["tweet_url"],
#                     created_at=str(t["created_at"]),
#                     mapped_keys=t["mapped_keys"]
#                 )
#                 to_add.append(this_tweet.asJson())
#         else:
#             curr_tweet = ParsedTweet(
#                 tweet_author=t["tweet_author"],
#                 tweet_id=float(t["tweet_id"]),
#                 num_replacements=t["num_replacements"],
#                 original_text=t["original_text"],
#                 modified_text=t["modified_text"],
#                 tweet_url=t["tweet_url"],
#                 created_at=str(t["created_at"]),
#                 mapped_keys=t["mapped_keys"]
#             )
#             to_add.append(curr_tweet.asJson())
#
#     num_added = len(to_add)
#     db.insert_many(to_add)
#     return num_added


# def update_tweet_spreadsheet(worksheet, tweetData, filter_unseen=True, last_seen_id=-1):
#     unseen_tweets = tweetData
#     if filter_unseen:
#         if last_seen_id < 0:
#             last_seen_id = int(get_most_recent_tweet_id_from_worksheet(worksheet))
#         unseen_tweets = list(
#             filter(lambda x: filter_seen_tweets(worksheet=worksheet, tweet=x, max_seen_id=str(last_seen_id)), tweetData)
#         )
#     to_display = list(map(lambda x: [x["tweet_author"],
#                                      x["tweet_url"],
#                                      x["num_replacements"],
#                                      x["original_text"],
#                                      x["modified_text"],
#                                      x["tweet_id"],
#                                      str(x["created_at"]),
#                                      ', '.join(x["mapped_keys"])], unseen_tweets))
#     firstBlankRow = get_first_blank_row(worksheet)
#     startCell = ''.join([firstBlankRow[0], str(firstBlankRow[1])])
#     worksheet.update(''.join(startCell), to_display)
#     return len(to_display)
