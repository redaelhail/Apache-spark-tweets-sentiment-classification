import tweepy
from kafka import KafkaProducer
import json
import logging
from datetime import datetime, timezone
import time
import yaml

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TwitterKafkaSearch:
    def __init__(self, bearer_token, bootstrap_servers='localhost:9092', topic='tweets'):
        """
        Initialize Twitter Search to Kafka pipeline
        
        Args:
            bearer_token (str): Twitter API Bearer Token
            bootstrap_servers (str): Kafka bootstrap servers
            topic (str): Kafka topic to produce to
        """
        self.topic = topic
        
        # Initialize Kafka Producer
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=3
        )
        
        # Initialize Twitter Client
        self.client = tweepy.Client(
            bearer_token=bearer_token,
            wait_on_rate_limit=True  # Important for free tier
        )

    def process_tweet(self, tweet):
        """Process and send tweet to Kafka"""
        try:
            tweet_data = {
                'id': tweet.id,
                'text': tweet.text,
                'created_at': tweet.created_at.isoformat() if tweet.created_at else None,
                'author_id': tweet.author_id,
            }
            
            # Send to Kafka
            self.producer.send(self.topic, tweet_data)
            logger.info(f"Tweet sent to Kafka: {tweet.text[:100]}...")
            
        except Exception as e:
            logger.error(f"Error processing tweet: {str(e)}")

    def search_tweets(self, query, max_results=10):
        """
        Search for tweets with given query
        
        Args:
            query (str): Search query
            max_results (int): Maximum number of results to fetch (default: 10)
                             Free tier allows up to 100 tweets per request
        """
        try:
            # Search tweets
            tweets = self.client.search_recent_tweets(
                query=query,
                max_results=max_results,
                tweet_fields=['created_at', 'author_id']
            )
            
            if not tweets.data:
                logger.info("No tweets found for the query")
                return
            
            # Process each tweet
            for tweet in tweets.data:
                self.process_tweet(tweet)
                
        except Exception as e:
            logger.error(f"Error searching tweets: {str(e)}")

    def run_periodic_search(self, query, interval_seconds=60, max_results=10):
        """
        Run periodic searches
        
        Args:
            query (str): Search query
            interval_seconds (int): Seconds to wait between searches
            max_results (int): Maximum results per search
        """
        logger.info(f"Starting periodic search for: {query}")
        while True:
            try:
                self.search_tweets(query, max_results)
                logger.info(f"Waiting {interval_seconds} seconds until next search...")
                time.sleep(interval_seconds)
            except KeyboardInterrupt:
                logger.info("Stopping periodic search")
                break
            except Exception as e:
                logger.error(f"Error in periodic search: {str(e)}")
                time.sleep(interval_seconds)  # Still wait before retrying

if __name__ == "__main__":


    # Twitter API credentials
    # load
    with open("data_ingestion/config.yaml") as stream:
        try:
            token = yaml.safe_load(stream)
            BEARER_TOKEN = token['BEARER_TOKEN']
        except yaml.YAMLError as exc:
            print(exc)
    
    
    # Initialize search client
    twitter_search = TwitterKafkaSearch(BEARER_TOKEN)
    
    # Create search query
    search_query = "(Python OR Spark) -is:retweet lang:en"
    
    # Run periodic search every 5 minutes
    # Free tier allows 180 requests per 15 minutes
    twitter_search.run_periodic_search(
        query=search_query,
        interval_seconds=300,  # 5 minutes
        max_results=10  # Adjust based on your monthly tweet cap
    )