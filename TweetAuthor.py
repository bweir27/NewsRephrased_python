import json
from constants import BASE_URL


class TweetAuthor:
    def __init__(self, author_id: str or int, name: str, username: str):
        if not isinstance(author_id, str):
            author_id = str(author_id)
        self.author_id = author_id
        self.name = name
        self.username = username
        self.formatted_name = f'{self.name} (@{self.username})'
        self.url = f'{BASE_URL}{self.username}'

    def __str__(self):
        return json.dumps(self.as_json())

    def __repr__(self):
        return json.dumps(self.as_json())

    def as_json(self):
        return {
            "_id": str(self.author_id),
            "author_id": self.author_id,
            "name": self.name,
            "username": self.username,
            "formatted": self.formatted_name,
            "author_url": self.url
        }
