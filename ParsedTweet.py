import datetime
from TweetAuthor import TweetAuthor
from replacement_filter import apply_replacement_filter


class ParsedTweet:
    def __init__(self, author: TweetAuthor, tweet_id: float or str, original_text: str, created_at: datetime.datetime,
                 tweet_url: str = '', mapped_keys=None, modified_text: str = '', num_replacements: int = 0,
                 been_posted: bool = False):
        if not isinstance(author, TweetAuthor):
            raise Exception(f'Invalid type for \"author\" (expected TweetAuthor, received {type(author)}')

        self.author_id = str(author.author_id)
        self.author = author
        self.tweet_id = str(tweet_id)
        if mapped_keys is None or len(str(modified_text)) == 0 or int(num_replacements) == 0:
            filter_info = apply_replacement_filter(str(original_text))
            mapped_keys = filter_info["replaced_keys"]
            modified_text = filter_info["modified_text"]
            original_text = filter_info["original_text"]
            num_replacements = filter_info["num_replacements"]
        self.num_replacements = int(num_replacements)
        self.original_text = str(original_text)
        self.modified_text = str(modified_text)
        self.tweet_url = str(tweet_url)
        if not isinstance(created_at, datetime.datetime):
            raise Exception(f'Invalid type for \"created_at\" field: {type(created_at)}')
        self.created_at = created_at
        self.mapped_keys = mapped_keys
        self.posted = been_posted
        self.json = self.as_json()
        self.seen_obj = self.seen_tweet()
        self.is_eligible = bool(self.num_replacements > 0)

    def __str__(self):
        return self.json

    def __repr__(self):
        return str(self)

    def post_tweet(self, value=True):
        self.posted = value
        self.json = self.as_json()

    def as_json(self):
        return {
            "author": self.author.as_json(),
            "tweet_id": str(self.tweet_id),
            "num_replacements": int(self.num_replacements),
            "original_text": str(self.original_text),
            "modified_text": str(self.modified_text),
            "tweet_url": str(self.tweet_url),
            "created_at": self.created_at,
            "mapped_keys": self.mapped_keys,
            "posted": self.posted
        }

    def seen_tweet(self):
        return {
            "author": self.author.as_json(),
            "tweet_id": str(self.tweet_id),
            "created_at": self.created_at,
            "tweet_url": self.tweet_url,
            "valid_tweet": bool(self.num_replacements > 0)
        }
