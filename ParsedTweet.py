import datetime
import json
import pprint
from functools import reduce

from TweetAuthor import TweetAuthor
from constants import TWEET_CHAR_LIMIT
from replacement_filter import apply_replacement_filter


class ParsedTweet:
    def __init__(self, author: TweetAuthor, tweet_id: float or str, original_text: str, created_at: datetime.datetime,
                 tweet_url: str = '', repl_freq_map: list = None, num_replacements: int = 0, modified_text: str = '',
                 been_posted: bool = False):
        if not isinstance(author, TweetAuthor):
            raise Exception(f'Invalid type for \"author\" (expected TweetAuthor, received {type(author)}')
        self._id = str(tweet_id)
        self.author_id = str(author.author_id)
        self.author = author
        self.tweet_id = str(tweet_id)
        if len(str(modified_text)) == 0 or repl_freq_map is None or len(repl_freq_map) == 0:
            filter_info = apply_replacement_filter(str(original_text))
            modified_text = filter_info["modified_text"]
            original_text = filter_info["original_text"]
            num_replacements = filter_info["num_replacements"]
            repl_freq_map = filter_info["replaced_key_freq"]
        self.num_replacements = num_replacements
        self.original_text = str(original_text)
        # TODO: handle len(modified_text) > TWEET_CHAR_LIMIT
        self.modified_text = str(modified_text)
        self.tweet_url = str(tweet_url)
        if not isinstance(created_at, datetime.datetime):
            raise Exception(f'Invalid type for \"created_at\" field: {type(created_at)}')
        self.created_at = created_at
        self.mapped_key_list = repl_freq_map
        self.posted = been_posted
        self.is_eligible = bool(self.num_replacements > 0 and len(self.modified_text) <= TWEET_CHAR_LIMIT)
        # self.json = self.as_json()
        # self.seen_obj = self.seen_tweet()

    def __str__(self):
        return json.dump(self.as_json())

    def __repr__(self):
        return str(self)

    def post_tweet(self, value=True):
        self.posted = value

    def as_json(self):
        return {
            "_id": str(self.tweet_id),
            "author": self.author.as_json(),
            "tweet_id": str(self.tweet_id),
            "num_replacements": int(self.num_replacements),
            "original_text": str(self.original_text),
            "modified_text": str(self.modified_text),
            "tweet_url": str(self.tweet_url),
            "created_at": self.created_at,
            "mapped_key_list": self.mapped_key_list,
            "posted": self.posted,
        }

    def seen_tweet(self):
        return {
            "author": self.author.as_json(),
            "tweet_id": str(self.tweet_id),
            "created_at": self.created_at,
            "tweet_url": self.tweet_url,
            "valid_tweet": bool(self.is_eligible)
        }
