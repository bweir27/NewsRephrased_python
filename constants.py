# ======== General constants ========
REFRESH_DB_DESC = 'Refreshes the Mongo database of parsed Tweets before running.'
REFRESH_DB_ENABLED = True
REFRESH_DB_DISABLED = False

REFRESH_WKS_DESC = 'Refreshes the Google Spreadsheets before running.'
REFRESH_WKS_ENABLED = True
REFRESH_WKS_DISABLED = False

REFRESH_ALL_DESC = 'Short-hand for using both \"--refresh-db\" and \"--refresh-wks\" arguments together.'
REFRESH_ALL_ENABLED = True
REFRESH_ALL_DISABLED = False

INTERVAL_DESC = 'An \'int\' value representing the time interval (in minutes) to wait between runs.'
ENFORCE_MIN_INTERVAL = True
IGNORE_MIN_INTERVAL = False

DEBUG_LOG_DESC = 'Shows debugging logs in output'
DEBUG_LOGS_ENABLED = True
DEBUG_LOGS_DISABLED = False

DEFAULT_INTERVAL_MINUTES = 30
MIN_INTERVAL_SECONDS = 120
USE_DEV_ENV = False
SCRIPT_DESCRIPTION = ''
EPILOG = ''


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


# ======== Twitter constants ========
BASE_URL = "https://twitter.com/"


# ======== MongoDB Constants ========
MONGO_HOST = 'localhost'
MONGO_PORT = 27017
DB_NAME = "news_rephrased"
DB_TWEET_COLLECTION_NAME = "tweets"
DB_SEEN_COLLECTION_NAME = "seen_tweets"
DB_AUTHORS_COLLECTION_NAME = "known_authors"

