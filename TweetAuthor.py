class TweetAuthor:
    def __init__(self, author_id: str or int, name: str, username: str):
        if not isinstance(author_id, str):
            author_id = str(author_id)
        self.author_id = author_id
        self.name = name
        self.username = username
        self.formatted_name = f'{self.name} (@{self.username})'
        self.json = self.as_json()

    def __str__(self):
        return str(self.json)

    def __repr__(self):
        return self

    def as_json(self):
        return {
            "author_id": self.author_id,
            "name": self.name,
            "username": self.username,
            "formatted": self.formatted_name
        }
