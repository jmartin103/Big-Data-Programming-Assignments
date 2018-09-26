import pyspark
from pyspark.context import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.functions import *
import json
import os

conf = SparkConf()
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

spark = SparkSession \
    .builder \
    .appName("Tweet Analysis") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# Use Spark DataFrames to visualize input tables
DF1 = spark.read.json("tweets.json")
DF2 = spark.read.json("cityStateMap.json")

# Show input tables
DF1.show()
DF2.show()

# Implement a third DataFrame to join the two tables
DF3 = DF1.join(DF2, DF1.geo == DF2.city, 'inner').drop(DF2.city)
DF3.show()

# Fourth DataFrame to count the number of tweets in each state
DF4 = DF3.groupBy("state").count()
DF4.show()

DF4.write.json("TweetsOfEachState.jsonl")
