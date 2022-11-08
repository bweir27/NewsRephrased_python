import datetime
import time

from helpers.helpers import mark_tweet_as_posted, get_tweet_url
from helpers.twitter_helpers import *
from helpers.mongo_helpers import *
from wordmap import WORD_MAP
import logging


def get_post_db_obj(tweet_res_data, reply_res_data, tweet_obj):
    thread_id = str(tweet_res_data['id'])
    quoted_tweet_id = str(tweet_obj['_id'])
    reply_id = str(reply_res_data['id'])
    quoted_author_username = str(tweet_obj['author']['username'])
    quoted_author_id = str(tweet_obj['author']['author_id'])
    thread_url = get_tweet_url(username=ACCOUNT_USERNAME, tweet_id=thread_id)
    quoted_tweet_url = get_tweet_url(username=quoted_author_username, tweet_id=quoted_tweet_id)
    reply_url = get_tweet_url(tweet_id=reply_id, username=ACCOUNT_USERNAME)

    post_obj = {
        "_id": quoted_tweet_id,
        "thread_id": thread_id,
        "quoted_id": quoted_tweet_id,
        "quoted_author_id": quoted_author_id,
        "quoted_author_username": quoted_author_username,
        "thread_url": thread_url,
        "quoted_tweet_url": quoted_tweet_url,
        # TODO:
        #   "thread_timestamp": post_timestamp,
        #   "quoted_timestamp": quoted_timestamp,
        #   "reply_timestamp": reply.created_at,
        "reply_id": reply_id,
        "reply_url": reply_url
    }
    return post_obj


def format_reply_text(mongo_tweet_obj):
    reply_txt = "Replaced terms:"
    mapped_keys = list(map(lambda x: x["key"], mongo_tweet_obj["mapped_key_list"]))
    for k in mapped_keys:
        reply_txt += f"\n\"{k}\" -> \"{WORD_MAP[k]}\""
    #     append part of ID to avoid posting exact duplicate tweets
    #       (which get blocked by Twitter's API)
    reply_txt += f"\n(id: {str(mongo_tweet_obj['tweet_id'])[-5:]})"
    return reply_txt


def post_tweet(tweet, show_output: bool = False, use_prod: bool = False):
    twitter_client = init_twitter_client()
    tweet_id = tweet["tweet_id"]
    to_quote = tweet
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
            mark_tweet_as_posted(tweet_id=quote_id, use_prod=use_prod)
            time.sleep(2)
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

            post_obj = get_post_db_obj(
                tweet_res_data=t_res.data,
                reply_res_data=reply_res.data,
                tweet_obj=to_quote)
            # add to Mongo posted_tweet collection
            insert_res = insert_posted_tweet_to_db(
                posted_obj=post_obj,
                use_prod=use_prod
            )
            return t_res, reply_res, insert_res
        else:
            print(f"Invalid Post: text too long: {len(mod_txt)}")
    else:
        print("Post Invalid: Tweet has already been posted")
