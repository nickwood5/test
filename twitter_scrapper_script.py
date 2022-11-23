import snscrape.modules.twitter as sntwitter
import pandas as pd
from datetime import datetime, timezone
import sys


coins = [("Bitcoin", "BTC"),
    ("Bitcoin Cash", "BCH"),
    ("Binance Coin", "BNB"),
    ("EOS.IO", "EOS"),
    ("Ethereum Classic", "ETC"),
    ("Ethereum", "ETH"),
    ("Litecoin", "LTC"),
    ("Monero", "XMR"),
    ("TRON", "TRX"),
    ("Stellar", "XLM"),
    ("Cardano", "ADA"),
    ("IOTA", "MIOTA"),
    ("Maker", "MKR"),
    ("Dogecoin", "DOGE")
]
 
twitter_following_limit = 5000 # based on https://help.twitter.com/en/using-twitter/twitter-follow-limit

def is_spam(tweet):
    if tweet.user.verified: 
        return False
    
    return (tweet.user.friendsCount == twitter_following_limit and tweet.user.followersCount < tweet.user.friendsCount) \
        or (tweet.user.friendsCount < tweet.user.followersCount * 0.1) \
        or (tweet.user.statusesCount / (datetime.now(timezone.utc) - tweet.user.created).days > 200) # avgs more than 200 tweets per day


def get_tweets(query, max_num_tweets):
    """ Gets at most max_num_tweets non-spam tweets matching query

    Args:
        query: twitter search query 
                (info on query: https://developer.twitter.com/en/docs/twitter-api/tweets/search/integrate/build-a-query)
        max_num_tweets: maximum number of tweets

    Returns: list of tweets and its data (structure of data: https://miro.medium.com/max/1400/1*b7499m8QPju3AH7WUreP2A.png
                specifically: {url, date, content, renderedContent, id, user, outlinks, tcooutlinks, replyCount, retweetCount, 
                likeCount, quoteCount, converstationId, lang, source, media, retweetedTweet, quotedTweet, mentionedUsers})
             number of spam tweets that were ignored
    """
    tweets_generator = sntwitter.TwitterSearchScraper(query).get_items()
    num_spam_tweets = 0
    tweets_list = []
    for _, tweet in enumerate(tweets_generator):
        if len(tweets_list) >= max_num_tweets: 
            break
        
        if not is_spam(tweet):
            #print("Tweet {}".format(tweet.url))
            tweets_list.append([tweet.url, tweet.date, tweet.rawContent, tweet.id, tweet.user, tweet.replyCount, 
                            tweet.retweetCount, tweet.likeCount, tweet.quoteCount, tweet.source])
        else:
            num_spam_tweets += 1
    
    return (tweets_list, num_spam_tweets)


def create_tweets_plk(start_date, end_date, max_tweets_per_coin_per_hour = 100):
    daterange = pd.date_range(start_date, end_date, freq="1H").map(pd.Timestamp.timestamp).map(int)

    rows = []

    num_intervals = len(range(len(daterange) - 1, 0, -1))
    print("Number of hourly intervals: ", num_intervals)
        
    for i in range(len(daterange) - 1, 0, -1):
        for coin_name, coin_ticker in coins:
            keyword = f'{coin_name} OR {coin_ticker}' # case insenstive
            since_time, until_time = daterange[i - 1], daterange[i]
            tweets, _ = get_tweets(f'{keyword} lang:en since_time:{since_time} until_time:{until_time}', max_tweets_per_coin_per_hour)
            tweet_df = pd.DataFrame(tweets, columns=["url", "date", "content", "id", "user", "replyCount", "retweetCount",
                                        "likeCount", "quoteCount", "source"])
            tweet_df.rename(columns={"id": "tweetId", "url": "tweetUrl", "source": "machineType"}, inplace=True)
            tweet_df.drop_duplicates(subset=["tweetId"], inplace=True)
            
            tweet_df["coinName"] = coin_name
            tweet_df["coinTicker"] = coin_ticker
            rows.append(tweet_df)
        if ((i + 1) % 10) == 0:
            print(f"{i + 1}/{num_intervals} hourly intervals left")
        
        
    df = pd.concat(rows)
    df["followersCount"] = df.apply(lambda e: e["user"].followersCount, axis=1)
    df["friendsCount"] = df.apply(lambda e: e["user"].friendsCount, axis=1)
    df["user"] =  df.apply(lambda e: e["user"].username, axis=1)

    print(f"{len(df)} number of tweets collected")
    print(f"file created: {start_date}:{end_date}--tweets.plk")
    df.to_pickle(f"{start_date}:{end_date}--tweets.plk")


if __name__ == "__main__":
    if (len(sys.argv) != 3):
        print("Usage: `python3 twitter_scrapper_script.py <start-date>`<end-date>", 
            "dates must be in YYYY-MM-DD format", 
            "start-date must be earlier than end-date", 
            "start-date is inclusive, end-date is exclusive", 
            sep="\n\t")
        sys.exit()
        
    start_date, end_date = sys.argv[1], sys.argv[2]
    print(f"start date (inclusive): {start_date}, end date (exclusive): {end_date}")
    ts_index = pd.date_range(start=start_date, end=end_date, freq='D')

    if len(ts_index) < 10:
        print("Scraping 1 date range")
        create_tweets_plk(start_date, end_date)
    else:
        print("Breaking date range into segments of size 10")
        num_intervals = len(ts_index) // 10

        ranges = []
        for i in range (num_intervals):
            ranges.append(ts_index[i*10:i*10 + 11])

        if (len(ts_index)) % 10 != 0:
            ranges.append(ts_index[(len(ts_index) // 10)*10:len(ts_index)])

        print("Scraping {} date ranges:".format(len(ranges)))
        print(ranges)
        for i, r in enumerate(ranges):
            print("{}: start date (inclusive): {}, end date (exclusive): {}".format(i + 1, r[0], r[-1]))

        for i, r in enumerate(ranges):
            print("Scraping start date (inclusive): {}, end date (exclusive): {}".format(r[0], r[-1]))
            create_tweets_plk(r[0], r[-1])
