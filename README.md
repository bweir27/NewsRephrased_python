# [@NewsRephrased](https://twitter.com/NewsRephrased)

Inspired by the [xkcd Substitution series](https://www.explainxkcd.com/wiki/index.php/Category:Substitution_series),
[NewsRephrased](https://twitter.com/NewsRephrased) is a Twitter bot that puts tweets from news media accounts through a word-replacement filter to create a satirized version
of their original tweet. Some of the [eligible tweets](#tweet-eligibility) will then be posted.

The replaced words ([WordMapping](https://github.com/bweir27/NewsRephrased_python/blob/master/wordmap.py)), the Eligible Tweets, Overall Stats, and [Blocked Terms](https://github.com/bweir27/NewsRephrased_python/blob/master/blocked_terms.py) can all be found on the
[NewsRephrased Google Spreadsheet](https://docs.google.com/spreadsheets/d/184VhgNxvHaDhimu-2o_ju14pb_WOE-izR7KKbgwgI0I/edit?usp=sharing).
The list of Tweet authors can also be found on the aforementioned Overall Stats page of the NewsRephrased Spreadsheet.


## Tweet Eligibility

In order for a tweet to be considered eligible, it must:
1. Have been posted on or since July 5th, 2022 (the date this project was started);
2. Be an original Tweet (not a retweet or a reply);
3. **Not** contain any of the [Blocked Terms](https://github.com/bweir27/NewsRephrased_python/blob/master/blocked_terms.py); and
4. Contain at least one of the words found in the [WordMap](https://github.com/bweir27/NewsRephrased_python/blob/master/wordmap.py)