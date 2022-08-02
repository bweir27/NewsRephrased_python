import datetime
import pprint
import signal

from helpers.helpers import mark_tweet_as_posted
from helpers.twitter_helpers import *
from helpers.mongo_helpers import *
from wordmap import WORD_MAP
import logging
import threading


def format_reply_text(mongo_tweet_obj):
    reply_txt = "Replaced terms:"
    mapped_keys = list(map(lambda x: x["key"], mongo_tweet_obj["mapped_key_list"]))
    for k in mapped_keys:
        reply_txt += f"\n\"{k}\" -> \"{WORD_MAP[k]}\""
    #     append part of ID to avoid posting exact duplicate tweets
    #       (which get blocked by Twitter's API)
    reply_txt += f"\n(id: {str(mongo_tweet_obj['tweet_id'])[-4:]})"
    return reply_txt


def post_tweet(tweet, show_output: bool = False):
    twitter_client = init_twitter_client()
    tweet_id = tweet["tweet_id"]
    to_quote = tweet  # get_mongo_tweet_by_id(tweet_id="1551647256385916930")  # 1547210751554818048
    mod_txt = str(to_quote["modified_text"])
    quote_id = str(to_quote["tweet_id"])
    if to_quote["posted"] is False:
        if show_output:
            print(f"Post Tweet:\t{to_quote}")
        if len(mod_txt) < TWEET_CHAR_LIMIT:
            t_res = twitter_client.create_tweet(
                text=mod_txt,
                quote_tweet_id=quote_id
            )
            if show_output:
                print('Original Posted.')
                print(t_res.data)
            #  Mark Tweet as posted in eligible_tweet db
            mark_tweet_as_posted(tweet_id=quote_id)
            reply_text = format_reply_text(to_quote)
            if show_output:
                print(f"\"{reply_text}\"")
                print(f"Response len:\t{len(reply_text)}")
            reply_res = twitter_client.create_tweet(
                text=reply_text,
                in_reply_to_tweet_id=t_res.data["id"]
            )
            if show_output:
                print('Reply Posted.')
                print(reply_res.data)
        else:
            print(f"Invalid Post: text too long: {len(mod_txt)}")
    else:
        print("Post Invalid: Tweet has already been posted")
