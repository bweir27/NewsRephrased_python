import gspread
from TweetAuthor import TweetAuthor
from constants import *
from helpers.mongo_helpers import init_mongo_client, get_key_freq_map, count_total_in_seen_db, \
    count_num_eligible_in_db, count_seen_eligible_by_author_list
from wordmap import WORD_MAP
from blocked_terms import BLOCKED_TERMS


def init_google_drive_clients():
    gc = gspread.service_account('credentials.json')
    sh = gc.open(SPREADSHEET_NAME)
    wordmap_wks = sh.get_worksheet(WORDMAP_WORKSHEET_NUM)
    tweet_suggestion_wks = sh.get_worksheet(SUGGESTION_WORKSHEET_NUM)
    stats_wks = sh.get_worksheet(STATS_WORKSHEET_NUM)
    blocked_term_wks = sh.get_worksheet(BLOCKED_TERM_WORKSHEET_NUM)
    return [wordmap_wks, tweet_suggestion_wks, stats_wks, blocked_term_wks]


def get_first_blank_row(worksheet, show_output: bool = False):
    source_col = SUGGESTION_COL_VALS["source"]
    id_col = worksheet.col_values(1)
    rowNum = len(id_col) + 1
    if show_output:
        print(f'firstBlank: {source_col}{rowNum}')
    return [source_col, rowNum]


def get_most_recent_tweet_row(worksheet) -> int:
    first_blank_row = get_first_blank_row(worksheet)
    return int(first_blank_row[1]) - 1


def get_most_recent_tweet_id_from_worksheet(worksheet):
    most_recent_row = get_most_recent_tweet_row(worksheet)
    target_cell = ''.join([SUGGESTION_COL_VALS["tweet_id"], str(most_recent_row)])
    tweet_id = str(worksheet.acell(target_cell).value)
    return str(tweet_id)


def get_most_recent_tweet_url_from_worksheet(worksheet):
    first_blank_row = get_first_blank_row(worksheet)
    most_recent_row = [first_blank_row[0], int(first_blank_row[1]) - 1]
    if most_recent_row[1] < SUGGESTION_WORKSHEET_STARTROW:
        print('ERROR')
        return
    target_cell = ''.join([SUGGESTION_COL_VALS["tweet_url"], str(most_recent_row[1])])
    tweet_url = worksheet.acell(target_cell).value
    return tweet_url


def tweet_has_been_seen_in_worksheet(worksheet, tweet, max_seen_id="0") -> bool:
    if "tweet_id" not in tweet:
        if "id" not in tweet:
            raise Exception('Invalid Tweet')
        id_val = str(tweet["id"])
    else:
        id_val = str(tweet["tweet_id"])
    last_seen_tweet_id = max_seen_id
    if max_seen_id < "1":
        last_seen_tweet_id = get_most_recent_tweet_id_from_worksheet(worksheet)
    return id_val < last_seen_tweet_id


def unseen_tweet(worksheet, tweet, max_seen_id="0") -> bool:
    return not tweet_has_been_seen_in_worksheet(worksheet=worksheet, tweet=tweet, max_seen_id=max_seen_id)


def filter_seen_tweets(worksheet, tweet, max_seen_id="0"):
    return unseen_tweet(worksheet=worksheet, tweet=tweet, max_seen_id=max_seen_id)


def update_blocked_term_wks(worksheet=None, show_output: bool = False):
    to_add = list()
    start_cell = BLOCKED_TERM_WORKSHEET_START_CELL
    blocked_wks = worksheet
    if blocked_wks is None:
        blocked_wks = init_google_drive_clients()[BLOCKED_TERM_WORKSHEET_NUM]
    to_add = list(map(lambda x: [str(f"\"{x}\"")], list(BLOCKED_TERMS)))
    if show_output:
        print(f'Blocked terms:\t{BLOCKED_TERMS}')
        print(f'# blocked terms: {len(to_add)}')
        print('Updating Blocked Term Worksheet...')
    worksheet.batch_clear([f'{start_cell}:B{len(to_add) + 30}'])
    blocked_wks.update(start_cell, to_add)
    if show_output:
        print('Done.')


def update_wordmap_wks(worksheet=None, show_output: bool = False):
    to_add = list()
    start_cell = 'A2'
    wordmap_wks = worksheet
    key_freq_counts = get_key_freq_map()
    if wordmap_wks is None:
        wordmap_wks = init_google_drive_clients()[SUGGESTION_WORKSHEET_NUM]
    if show_output:
        print('Updating wordmap spreadsheet...', end='')
    for key in WORD_MAP:
        freq = key_freq_counts[key] if key in key_freq_counts else 0
        to_add.append([key, WORD_MAP[key], int(freq)])
    worksheet.batch_clear([f'{start_cell}:C{len(to_add)+20}'])
    wordmap_wks.update(start_cell, to_add)
    if show_output:
        print('done.')


def update_stats_wks(worksheet=None):
    stats_wks = worksheet
    if worksheet is None:
        stats_wks = init_google_drive_clients()[STATS_WORKSHEET_NUM]
    # TODO: refactor to just get per-author stats, then reduce() to derive # seen/eligible from there
    # get total number of "seen" tweets
    total_num_seen = count_total_in_seen_db()
    # get total number of "eligible" tweets
    total_num_eligible = count_num_eligible_in_db()
    # Get seen vs eligible stats on a per-author basis
    author_eligible_seen = count_seen_eligible_by_author_list()
    author_stats = list(map(lambda x: [x["formatted"], x["seen"], x["eligible"]], author_eligible_seen))
    # Update Stats worksheet page
    stats_wks.update_acell(STATS_WORKSHEET_NUM_SEEN_CELL, total_num_seen)
    stats_wks.update_acell(STATS_WORKSHEET_NUM_ELIGIBLE_CELL, total_num_eligible)
    stats_wks.update(STATS_WORKSHEET_AUTHOR_SEEN_ELIGIBLE_CELL, author_stats)
    return {
        "num_seen": total_num_seen,
        "num_eligible": total_num_eligible,
        "author_stats": author_stats
    }


def format_suggested_wks():
    suggest_wks = init_google_drive_clients()[SUGGESTION_WORKSHEET_NUM]
    max_col_letter = chr(ord('A') + len(SUGGESTION_HEADER_ROW) - 1)
    # format header row
    suggest_wks.format(
        'A1:I1',
        {
            'horizontalAlignment': 'CENTER',
            'textFormat': {
                'bold': True,
                'fontSize': 10,
            },
            'borders': {
                "top": HEADER_ROW_BORDER,
                "right": HEADER_ROW_BORDER,
                "bottom": HEADER_ROW_BORDER,
                "left": HEADER_ROW_BORDER
            }
        }
    )
    suggest_wks.update("A1", [SUGGESTION_HEADER_ROW])
    # format other cells
    suggest_wks.format(
        f'A2:H200',
        {
            "wrapStrategy": "WRAP"
        }
    )


def format_suggested_tweet_worksheet_row(db_tweet):
    source_col = f'{db_tweet["author"]["name"]} (@{db_tweet["author"]["username"]})'
    replaced_keys = ', '.join([str(x["key"]) for x in db_tweet["mapped_key_list"]])
    return [
        source_col,
        db_tweet["tweet_url"],
        db_tweet["num_replacements"],
        db_tweet["original_text"],
        db_tweet["modified_text"],
        replaced_keys,
        str(db_tweet["created_at"]),
        db_tweet["tweet_id"],
    ]


def update_suggested_tweet_wks(worksheet=None, partial_update: bool = True, show_output: bool = False, use_prod: bool = False) -> int:
    suggest_wks = worksheet
    start_cell = 'A2'
    to_add = list()

    # Connect to tweets DB
    db = init_mongo_client(use_prod=use_prod)
    tweet_db = db[DB_TWEET_COLLECTION_NAME]
    seen_db = db[DB_SEEN_COLLECTION_NAME]

    if suggest_wks is None:
        suggest_wks = init_google_drive_clients()[SUGGESTION_WORKSHEET_NUM]
    if show_output:
        print('\nUpdating SuggestedTweet spreadsheet...')
    if partial_update:
        latest_worksheet_tweet_id = str(get_most_recent_tweet_id_from_worksheet(suggest_wks))
        find_query = {
            "tweet_id": {"$gt": latest_worksheet_tweet_id}
        }
        known_tweets = tweet_db.find(find_query).sort("tweet_id", 1)
        num_found_tweets = tweet_db.count_documents(find_query)
        first_blank = get_first_blank_row(worksheet=suggest_wks, show_output=show_output)
        start_cell = ''.join(['A', str(first_blank[1])])
    else:
        known_tweets = tweet_db.find({}).sort("tweet_id", 1)
        num_found_tweets = tweet_db.count_documents({})
        num_seen_tweets = seen_db.count_documents({})
        #     Clear worksheet
        suggest_wks.update('A1', [SUGGESTION_HEADER_ROW])
        suggest_wks.batch_clear([f"A2:I{num_seen_tweets + 20}"])
    if show_output:
        print(f'FirstBlank: {start_cell}')
        print(f'Found {num_found_tweets} to add...')
    # Add the found tweets to the worksheet
    if num_found_tweets > 0:
        for t in known_tweets:
            to_add.append(t)
        to_display = list(map(format_suggested_tweet_worksheet_row, to_add))
        if show_output:
            print('Updating...', end='')
        suggest_wks.update(start_cell, to_display)
        if show_output:
            print('done.')
    return num_found_tweets


def update_worksheets(partial_update=True, show_output=False):
    google_clients = init_google_drive_clients()
    wordmap_worksheet = google_clients[WORDMAP_WORKSHEET_NUM]
    suggest_worksheet = google_clients[SUGGESTION_WORKSHEET_NUM]
    stats_wks = google_clients[STATS_WORKSHEET_NUM]
    blocked_wks = google_clients[BLOCKED_TERM_WORKSHEET_NUM]
    update_wordmap_wks(worksheet=wordmap_worksheet, show_output=show_output)
    update_suggested_tweet_wks(worksheet=suggest_worksheet, partial_update=partial_update, show_output=show_output)
    update_stats_wks(worksheet=stats_wks)
    update_blocked_term_wks(worksheet=blocked_wks, show_output=show_output)


def complete_refresh_spreadsheets(show_output=False):
    update_worksheets(partial_update=False, show_output=show_output)
