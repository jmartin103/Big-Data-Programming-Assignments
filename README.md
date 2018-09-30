# Big-Data-Tweet-Analysis

A Python program to analyze the number of tweets in each state. This program uses PySpark and Spark Configuration to read two JSON files: one contains the information for a user, the city, and the tweet, and then the second JSON file contains city and state information. The two JSON files are joined by the city column into a DataFrame, and then this DataFrame counts the number of tweets in each state.
