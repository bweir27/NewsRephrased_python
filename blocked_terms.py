"""
TW //




This is the list of words / phrases that, if found within the text of a tweet,
will mark the tweet as ineligible and prevent the Tweet from being put through the filter
(to avoid making 'light' of potentially gruesome or triggering situations)

This list will be regularly updated.


"""

BLOCKED_TERMS = {
    "shooting",
    "killed",
    "killing",
    "died",
    "dead",
    " dies",
    "fatally",
    "Uvalde",
    "massacre",
    "shooter",
    "murder",
    "strangled",

    "rape",
    "rapist",
    "sexual assualt",
    "sexually abuse",
    "sexual abuse",
    "sexual misconduct",
    "suicide",
    "torture",
    "anorexia",
    "anorexic",
    "minor",
    "children",
    "opioid",
    "overdose",
    "Jayland Walker",
    "George Floyd",
}
