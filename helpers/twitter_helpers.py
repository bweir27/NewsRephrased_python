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


def get_twitter_user(user_id=None, username=None, twitter_client=None):
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
            _id=twitter_user.id,
            name=twitter_user.name,
            username=twitter_user.username
        )
    elif username and isinstance(username, str):
        usr = get_twitter_user(username=username, twitter_client=twitter_client)
        if usr and usr.data:
            user = usr.data
            return TweetAuthor(
                _id=user.id,
                name=user.name,
                username=user.username
            )
