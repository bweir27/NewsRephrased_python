from helpers.google_helpers import *
from wordmap import *
from constants import *

# init Google Drive / Sheets clients
google_clients = init_google_drive_clients()
wordmap_wks = google_clients[WORDMAP_WORKSHEET_NUM]
suggest_wks = google_clients[SUGGESTION_WORKSHEET_NUM]


def update_wordmap_wks():
    print('Updating wordmap spreadsheet...', end='')
    to_add = list()
    start_cell = 'A2'
    for key in WORD_MAP:
        to_add.append([key, WORD_MAP[key]])
    wordmap_wks.update(start_cell, to_add)
    print('done.')


if __name__ == '__main__':
    print('Updating Google Spreadsheets...')
    update_wordmap_wks()
    update_suggested_tweet_wks(partial_update=False)
    print('Done.')

