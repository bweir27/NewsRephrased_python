SEED_DATA_CSV = "CSVs/NewsRephrased_SeededSuggestions.csv"


# ======== Google constants ========
SPREADSHEET_NAME = "NewsRephrased"
WORDMAP_WORKSHEET_NUM = 0
SUGGESTION_WORKSHEET_NUM = 1
SUGGESTION_WORKSHEET_STARTROW = 1
SUGGESTION_HEADER_ROW = [
    "Source",
    "URL",
    "# Replacements",
    "Original",
    "Revised",
    "Replaced Words",
    "Date Posted",
    "TweetID",
]

SUGGESTION_COL_VALS = {
    "source": 'A',
    "tweet_url": 'B',
    "num_replacements": 'C',
    "original_text": 'D',
    "modified_text": 'E',
    "mapped_keys": 'F',
    "tweet_date": 'G',
    "tweet_id": 'H',
}

"""
The default border style for the header row of the spreadsheets
For more information, see:
https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#cellformat
"""
HEADER_ROW_BORDER = {
  "style": "SOLID",
  "colorStyle": {
    "rgbColor": {
      "red": 0,
      "green": 0,
      "blue": 0,
      "alpha": 0
    }
  }
}

"""
These are the terms that map to each other in the replacement filter (see wordmap.py)
    Keep track of these to avoid re-/un-mapping these terms (resulting in a net-zero change in the text)
"""
WORDMAP_SWAP_CASES = [
    "years", "minutes",
    "Netflix", "Quibi",
    "international", "local"
]


# ======== Twitter constants ========
BASE_URL = "https://twitter.com/"


# ======== MongoDB Constants ========
MONGO_HOST = 'localhost'
MONGO_PORT = 27017
DB_NAME = "news_rephrased"
DB_TWEET_COLLECTION_NAME = "tweets"
DB_SEEN_COLLECTION_NAME = "seen_tweets"
DB_AUTHORS_COLLECTION_NAME = "known_authors"

