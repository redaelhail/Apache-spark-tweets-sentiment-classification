from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StringType, StructType, StructField, FloatType
from textblob import TextBlob

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("TwitterSentimentAnalysis") \
    .config("spark.sql.streaming.checkpointLocation", "./checkpoint") \
    .getOrCreate()

# Define schema for tweets
schema = StructType([
    StructField("id", StringType(), True),
    StructField("text", StringType(), True),
    StructField("created_at", StringType(), True)
])

# Read from Kafka
tweets_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "tweets") \
    .load() \
    .selectExpr("CAST(value AS STRING)")

# Parse JSON
tweets_df = tweets_df.withColumn("data", from_json(col("value"), schema)).select("data.*")

# Define UDF for sentiment analysis
def get_sentiment(text):
    return TextBlob(text).sentiment.polarity

sentiment_udf = udf(get_sentiment, FloatType())

# Apply sentiment analysis
tweets_df = tweets_df.withColumn("sentiment", sentiment_udf(col("text")))

# Write output to console
query = tweets_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
