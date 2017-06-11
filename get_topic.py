import sys,getopt,got,datetime,codecs 
import csv
import time
import ConfigParser 

def create_csvwriter(topic):
    #Open and close file to clear contents
    file = open("topic_" + topic + ".csv", "w").close()
    file = open("topic_" + topic + ".csv", "a")
    csvwriter = csv.writer(file, delimiter=",")
    return csvwriter

def get_interval(topic_zip, run, time0, time1):
    if run:
        this_topic, this_csvwriter = topic_zip[0], topic_zip[1]
        try:
            tweet_criteria = got.manager.TweetCriteria()
            tweet_criteria .querySearch = this_topic 
            tweet_criteria .since = str(time0)
            tweet_criteria .until = str(time1)
            tweet_criteria .maxTweets = -1
            tweets = got.manager.TweetManager.getTweets(tweet_criteria)
            for tweet in tweets:
                this_csvwriter.writerow([str(time0), tweet.text.encode("utf8")])
                #csvwriter.writerow(tweet)
        except KeyboardInterrupt:
            raise
        except:
            print("Get tweets about " + this_topic + " failed.  Will try again in 1s.")
            return False
    return True

def main():
        parser = ConfigParser.SafeConfigParser()
        parser.read('config.ini')
        start_year = int(parser.get("start_time", "year"))
        start_month = int(parser.get("start_time", "month"))
        start_day = int(parser.get("start_time", "day"))
        end_year = int(parser.get("end_time", "year"))
        end_month = int(parser.get("end_time", "month"))
        end_day = int(parser.get("end_time", "day"))
        topics = parser.get("topic", "topics")
        retry_attempts = int(parser.get("general", "retry_attempts"))
        topics_list = topics.split(",")
        topics_list = map(lambda x: x.strip(), topics_list)
        csvwriters = map(create_csvwriter, topics_list)

        print(topics_list)

        start_datetime = datetime.date(start_year, start_month, start_day)
        end_datetime = datetime.date(end_year, end_month, end_day)
        time0 = start_datetime
        time1 = start_datetime + datetime.timedelta(days=1)
        attempts = retry_attempts
        for csvwriter in csvwriters:
            csvwriter.writerow(["date", "text"])

        while time1 < end_datetime:
            print("Starting to get tweets")
            print("time0: " + str(time0))
            print("time1: " + str(time1))
            if retry_attempts == attempts:
                run = [True] * len(topics_list)
            if retry_attempts < 1:
                print("Too many retry attempts.")
                return
            topic_zip = zip(topics_list, csvwriters) 
            run = map(lambda x: get_interval(x, run, time0, time1), topic_zip)
            if all(x for x in run):
                time0 = time1 
                time1 += datetime.timedelta(days=1)
                retry_attempts = retry_attempts
            else:
                retry_attempts -= 1

if __name__ == "__main__":
    main()
