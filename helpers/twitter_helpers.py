import tweepy
from TweetAuthor import TweetAuthor
from twittercreds import *


def init_twitter_client():
    return tweepy.Client(
        bearer_token=twitter_bearer_token,
        consumer_key=consumer_key,
        consumer_secret=consumer_secret,
        access_token=access_token,
        access_token_secret=access_token_secret,
        wait_on_rate_limit=True
    )


def get_user_most_recent_tweet(target: tweepy.User = None, target_id: str = None):
    twitter_client = init_twitter_client()
    if target is None and target_id is None:
        raise Exception("Must provide at least one value")
    if target is None:
        t_usr = get_twitter_user(user_id=target_id, twitter_client=twitter_client)
        if t_usr and t_usr.data:
            target = t_usr.data
        else:
            raise Exception("Invalid User ID")
    raw_tweet_objs = list()
    most_recent = twitter_client.get_users_tweets(
        id=target.id,
        exclude=["retweets", "replies"],
        max_results=5,
        tweet_fields=["id", "text", "created_at"],
        expansions=["author_id"],
        user_fields=["username"]
    )
    if most_recent.meta["result_count"] > 0:
        raw_tweet_objs.extend(most_recent.data)
        id_list = list(map(lambda x: x["id"], raw_tweet_objs))
        return raw_tweet_objs[0]
    return None


def get_user_recent_tweets(twitter_client: tweepy.Client, target: tweepy.User, most_recent_id: str or float or int = None, max_results: int = 100) -> list:
    if most_recent_id is None:
        # if no specified most_recent_id (if new author), only retrieve 5 most recent
        max_results = 5
    if max_results > 100:
        max_results = 100
    elif max_results < 5:
        max_results = 5
    recent_tweets = twitter_client.get_users_tweets(
                id=target.id,
                since_id=most_recent_id,
                exclude=["retweets", "replies"],
                max_results=max_results,
                tweet_fields=["id", "text", "created_at"],
                expansions=["author_id"],
                user_fields=["username"]
            )
    # If there are no new Tweets, Twitter / Tweepy API returns data=None (as opposed to an empty list)
    raw_tweet_objs = list()
    if recent_tweets.meta["result_count"] > 0:
        raw_tweet_objs.extend(recent_tweets.data)
        # Handle pagination
        while recent_tweets.meta["result_count"] > 0 and "next_token" in recent_tweets.meta and most_recent_id:
            next_page_token = recent_tweets.meta["next_token"]
            recent_tweets = twitter_client.get_users_tweets(
                id=target.id,
                since_id=most_recent_id,
                exclude=["retweets", "replies"],
                max_results=100,
                tweet_fields=["id", "text", "created_at"],
                expansions=["author_id"],
                user_fields=["username"],
                pagination_token=next_page_token
            )
            if recent_tweets.data:
                raw_tweet_objs.extend(recent_tweets.data)
    return raw_tweet_objs


def get_twitter_user(user_id: str or int or float = None, username: str = None, twitter_client=None):
    if all(v is None for v in [user_id, username]):
        raise Exception('Must provide at least one value')
    if username and not isinstance(username, str):
        raise Exception(f'Invalid username: Must be of type \'str\' (got type {type(username)}')
    if user_id and not isinstance(user_id, (str, int, float)):
        raise Exception('Invalid type for \"user_id\" argument '
                        f'(expected types [ str | int | float], received {type(user_id)}')
    if twitter_client is None or not isinstance(twitter_client, tweepy.Client):
        twitter_client = init_twitter_client()
    if user_id:
        u_id = str(user_id)
        return twitter_client.get_user(id=u_id)
    return twitter_client.get_user(username=username)


def parse_twitter_author(twitter_user=None, username=None, twitter_client=None):
    if twitter_user and isinstance(twitter_user, tweepy.User):
        return TweetAuthor(
            author_id=twitter_user.id,
            name=twitter_user.name,
            username=twitter_user.username
        )
    elif username and isinstance(username, str):
        usr = get_twitter_user(username=username, twitter_client=twitter_client)
        if usr and usr.data:
            user = usr.data
            return TweetAuthor(
                author_id=user.id,
                name=user.name,
                username=user.username
            )
