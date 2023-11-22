import evadb
import pandas as pd
import os

pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_columns', None)

os.environ["OPENAI_API_KEY"] = "<OPENAI_KEY>"
cursor = evadb.connect().cursor()

# connect to Reddit as datasource
params = {
    "client_id": "<CLIENT_ID>",
    "client_secret": "<CLIENT_SECRET>",
    "user_agent": "<USER_AGENT>",
    "subreddit": "gatech",
    "num_results": "10"
}
cursor.query("DROP DATABASE IF EXISTS reddit_data").df()
cursor.query(f"CREATE DATABASE reddit_data WITH ENGINE = 'reddit', PARAMETERS = {params}").df()

# generate emoji reaction
print(cursor.query("SELECT title, ChatGPT('React using an emoji to capture the sentiment of this', full_text) FROM reddit_data.top;").df())

# perform sentiment analysis and query specific sentiment
print(cursor.query("""SELECT id, title, score, ChatGPT(
                   "Use only one word, either 'positive' or 'negative', to capture the sentiment of this"
                   , full_text)
                    FROM reddit_data.top;""").df())
print(sentiment_select_df = cursor.query("""SELECT id, title FROM reddit_data.top
                   WHERE ChatGPT(
                   "Use only one word, either 'positive' or 'negative', to capture the sentiment of this"
                   , full_text) = 'positive'
                   AND score > 1100
                    """).df())
