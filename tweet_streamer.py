#!/usr/bin/python

from datetime import timedelta
import settings
import time
import tweepy
import sys

from rabbitmq_manager import PikaConnection

import simplejson as json

#import MySQLdb as db


auth = tweepy.OAuthHandler(settings.CONSUMER_KEY, settings.CONSUMER_SECRET)
auth.set_access_token(settings.ACCESS_KEY, settings.ACCESS_SECRET)
api = tweepy.API(auth)


class CustomStreamListener(tweepy.StreamListener):
    def __init__(self):
        super(CustomStreamListener, self).__init__()
        self.pc = PikaConnection()
        self.pc.connect()

    def on_status(self, tweet):
        created_at = tweet.created_at + timedelta(hours=7)

        print created_at
        print tweet.user.screen_name, tweet.user.location
        print tweet.text
        print

        tweet_model = (
            tweet.id_str,
            tweet.user.screen_name,
            created_at.isoformat(' '),
            str(tweet.text.encode('utf-8')),
            None,
            None
        )

        #db_insert_tweets(tweet_model)

        kwargs = { 'message': json.dumps(tweet_model) }
        self.pc.publish_message(**kwargs)


    def on_error(self, status_code):
        print >> sys.stderr, 'Encountered error with status code:', \
            status_code + ": Not Acceptable Response. One or more \
            inputs are wrong."
        return True

    def on_timeout(self):
        print >> sys.stderr, 'Timeout...'
        return True


#def db_insert_tweets(model):
#    """Push a tweet immediately"""
#    try:
#        conn = db.connect(settings.HOST, settings.USER,
#                          settings.PASS, settings.DB)
#    except db.ERROR, e:
#        print "Error %d: %s" % (e.args[0], e.args[1])
#    else:
#        cursor = conn.cursor()
#        sql_insert = "INSERT INTO Tweet(id_str, screen_name, created_at, \
#                        text, pic_url, geolocation) \
#                        VALUES(%s, %s, %s, %s, %s, %s)"
#        cursor.execute(sql_insert, model)
#    finally:
#        if conn:
#            conn.commit()
#            conn.close()


if __name__ == '__main__':
    while True:
        try:
            stream_api = tweepy.streaming.Stream(auth, CustomStreamListener(),
                                                 timeout=60)
            # fetch 'lewatmana', 'TMCPoldaMetro', 'NTMCLantasPolri',
            # 'RTMC_Jatim', 'RadioElShinta'
            #stream_api.filter(follow=['18795386', '76647722', '130169009'])
            stream_api.filter(track=['lulung'])
        except IOError, e:
            print e
        except KeyboardInterrupt:
            print "Stop the tweet streaming"
            quit()
        finally:
            print
            print "Twitter has resetted the connection. Wait for a while and" \
                "let's reconnect! >:)"
            time.sleep(5)
