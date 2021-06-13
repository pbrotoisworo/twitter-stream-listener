import tweepy
import os
import pandas as pd
import traceback

consumer_key = 'YRoCykGzWaoZJ5ehnPxQ0Hubc'
consumer_secret = 'XwTweV1RdrMyEqDFfuKX5eS8COSEOahNbK87wJJX4YFoLNF8Vg'
access_token = '225641768-97zmIlo1bOeVSE3nSWvWA4bLuMswbu20mD1wcPkk'
access_secret = 'YmBBHQ6vSmf4GeiX7GKx2Tx2a9E7hv7xAxTtWV6mODOuN'

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)
api = tweepy.API(auth, wait_on_rate_limit=True)
userConnection = True

class StreamListener(tweepy.StreamListener):

    def on_status(self, status):
        tweet = status._json
        print(tweet)
        
        print('==============================================')
        # Get creation datetime
        tweet_datetime = tweet['created_at']
        # Check if retweet
        # TODO: Refactor this where you detect if a tweet has > 280 characters.
        #       JSON structure changes depending on whether or not it is > 280 chars.
        if 'retweeted_status' in tweet.keys():
            tweet_is_retweet = True
            rt_original_author = tweet['retweeted_status']['user']['screen_name']
            rt_original_id = tweet['retweeted_status']['id_str']
            rt_source = f'https://twitter.com/{rt_original_author}/status/{rt_original_id}'
            rt_count = tweet['retweeted_status']['retweet_count']
            rt_fav_count = tweet['retweeted_status']['favorite_count']
            try:
                rt_text = tweet['retweeted_status']['extended_tweet']['full_text']
            except KeyError:
                rt_text = tweet['retweeted_status']['text']
        else:
            tweet_is_retweet = False
            rt_original_author = None
            rt_original_id = None
            rt_source = None
            rt_count = None
            rt_fav_count = None
            
        # Get tweet ID
        username = tweet['user']['screen_name']
        tweet_id = tweet['id_str']
        tweet_source = f'https://twitter.com/{username}/status/{tweet_id}'
        # Get hashtags
        tweet_hashtags = list()
        tweet_entities = tweet['entities']['hashtags']
        for item in tweet_entities:
            try:
                tweet_hashtags.append(item['text'].lower())
            except:
                # Probably number hashtag
                tweet_hashtags.append(item['text'])
        tweet_hashtags = ','.join(tweet_hashtags)

        # Get text
        # if 'extended_tweet' in tweet.keys():
        try:
            if not tweet_is_retweet:
                if 'extended_tweet' not in tweet.keys():
                    tweet_full_text = tweet['text']
                else:
                    tweet_full_text = tweet['extended_tweet']['full_text']
            # elif 'retweeted_status' in tweet.keys():
            elif tweet_is_retweet:
                tweet_full_text = rt_text
            else:
                tweet_full_text = tweet['text']
        except:
            traceback.print_exc()
            import json
            with open('json_stuff.txt', 'w') as f:
                # f.write(tweet)
                json.dump(tweet, f)
            import sys
            sys.exit()
        tweet_text = tweet['text']

        print('Datetime:', tweet_datetime)
        print('Tweet URL:', tweet_source)
        print('Hashtags:', tweet_hashtags)
        print('Text:', tweet_full_text)

        # Add to database
        df = pd.read_csv('tweets.csv')
        df = df.append(
            {
            'created': tweet_datetime,
            'is_rt': tweet_is_retweet,
            'rt_source': rt_source,
            'rt_count': rt_count,
            'rt_fav_count': rt_fav_count,
            'text': tweet_text,
            'full_text': tweet_full_text,
            'hashtags': tweet_hashtags,
            'source': tweet_source
            },
            ignore_index=True
        )
        df.to_csv('tweets.csv', index=False)
        # import json
        # with open('json_stuff.txt', 'w') as f:
        #     # f.write(tweet)
        #     json.dump(tweet, f)
        # import sys
        # sys.exit()
      

    def on_error(self, status_code):
        print(sys.stderr, 'Encountered error with status code:', status_code)
        return True # Don't kill the streams

    def on_timeout(self):
        print(sys.stderr, 'Timeout...')
        return True # Don't kill the stream
    
if __name__ == '__main__':
    tweets_file = 'tweets.csv'
    if not os.path.exists(tweets_file):
        df = pd.DataFrame(columns=['created', 'is_rt', 'rt_source', 'rt_count', 'rt_fav_count', 'text', 'full_text', 'hashtags', 'source'])
        df.to_csv(tweets_file, index=False)

    while True:
        try:
            streamingAPI = tweepy.streaming.Stream(auth, StreamListener())
            streamingAPI.filter(track=['#happy'])
        except KeyboardInterrupt:
            break
        except Exception as e:
            traceback.print_exc()
            continue