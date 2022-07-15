"""
Inspired by and built on top of:
https://www.explainxkcd.com/wiki/index.php/1288:_Substitutions
https://www.explainxkcd.com/wiki/index.php/1625:_Substitutions_2
https://www.explainxkcd.com/wiki/index.php/1679:_Substitutions_3
"""

# TODO: refactor these to be in RegEx form to prevent matches being found WITHIN certain words
#       (e.g., prevent cases like "disruption" -> "destroyion")
WORD_MAP = {
    "witnesses": "these dudes I know",
    "allegedly": "kinda probably",
    "new study": "tumblr post",
    "rebuild": "avenge",
    "space": "spaaace",
    "google glass": "virtual box",
    "smartphone": "pokédex",
    "electric": "atomic",
    "senator": "elf-lord",
    " car ": " cat ",
    "-car ": "-cat ",
    " car.": " cat.",
    " cars ": " cats ",
    "-cars ": "-cats ",
    " cars.": " cats.",
    "-cars.": "-cats.",
    "election": "eating contest",
    "congressional leaders": "river spirits",
    "homeland security": "homestar runner",
    "could not be reached for comment": "is guilty and everyone knows it",
    "debate": "dance-off",
    "self-driving": "uncontrollably swerving",
    # "poll": "psychic reading",
    " poll ": " psychic reading ",
    " poll": " psychic reading",
    "candidate": "airbender",
    "drone": "dog",
    "vows to": "probably won't",
    "at large": "very large",
    "successfully": "suddenly",
    "expands": "physically expands",
    "first-degree": "friggin' awful",
    "second-degree": "friggin' awful",
    "third-degree": "friggin' awful",
    "an unknown number": "like hundreds",
    "front runner": "blade runner",
    "global": "spherical",
    "year": "minute",
    "minute": "year",
    "no indication": "lots of signs",
    "urged restraint by": "drunkenly egged on",
    "horsepower": "tons of horsemeat",
    "gaffe": "magic spell",
    "ancient": "haunted",
    "star-studded": "blood-soaked",
    "remains to be seen": "will never be known",
    "silver bullet": "way to kill werewolves",
    "subway system": "tunnels I found",
    "surprising": "surprising (but not to me)",
    "war of words": "interplanetary war",
    "tension": "sexual tension",
    "cautiously optimistic": "delusional",
    "doctor who": "the Big Bang Theory",
    "win votes": "find pokemon",
    "behind the headline": "beyond the grave",
    "email": "poem",
    "facebook post": "poem",
    "tweets": "screams into pillow",
    "tweet": "poem",
    "facebook ceo": "this guy",
    "Jeff Bezos": "a hot single in your area",
    "latest": "final",
    "disrupt": "destroy",
    # "meeting": '',
    "scientist": "Channing Tatum",
    "scientists": "Channing Tatum and his friends",
    "you won't believe": "I'm really sad about",

    #     Additional
    "immigrants": "people",
    "migrants": "people",
    "police": "30-50 Feral Hogs",
    "Netflix": "Quibi",
    "Quibi": "Netflix",
    "international": "local",
    "local": "international",
    "donation": "bribe",
    "GoFundMe": "American Healthcare",
    "hundreds": "like... two (max)",
    "millions": 'an incalculable amount',
    "million": '',
    "billionaire": "oligarch",
    "billion": 'dozen',
    "students": "literal children",
    # "hackers": "keyboard warriors", # "clickity-clackity hackybois
    "hackers": "clickity-clackity hackybois",
    "Florida": "Depths of Hell",
    "abortion": "bodily autonomy",
    "Apple": "Nokia",
    "world": "neighborhood",
    "FBI": "Codename Kids Next Door",
    "interview": "heart-to-heart",
    "database": "diary",
    "monkeypox": "baby fever",
    "Kavanaugh": "Beer Enjoyer",
    "member": "fan",
    "Elon Musk": "Grimes\' ex",
    "gasoline": "weed",
    "gas ": "weed ",
    " gas ": " weed ",
    "breaking news": "HOLY SHIT"
}

"""
These are the terms that map to each other in the replacement filter (see wordmap.py)
    Keep track of these to avoid re-/un-mapping these terms (which would result in a net-zero change to the text)
"""
WORDMAP_SWAP_CASES = {
    "years", "minutes",
    "Netflix", "Quibi",
    "international", "local",
    "year", "minute",
}

