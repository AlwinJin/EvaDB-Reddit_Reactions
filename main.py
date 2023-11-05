import evadb
import pandas as pd
import praw
import re
import sqlalchemy
import os


pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_columns', None)

os.environ["OPENAI_KEY"] = "<OPENAI KEY>"
cursor = evadb.connect().cursor()

# Gets submissions and returns dataframe
def get_submissions():
    reddit = praw.Reddit(
        client_id="<REDDIT CLIENT_ID>",
        client_secret="<REDDIT CLIENT_SECRET>",
        user_agent="<REDDIT USER AGENT>"
    )

    subreddit = reddit.subreddit("gatech")
    top_posts = subreddit.top(time_filter="all", limit=10)

    # create df of top posts
    data = []
    for submission in top_posts:
        data.append([submission.id, submission.title, submission.selftext, submission.title + "\n " + submission.selftext, submission.url])

    pd.set_option('display.max_colwidth', None)
    # pd.set_option('display.max_columns', None)  
    df = pd.DataFrame(data, columns=['id', 'title', 'text', 'full_text', 'url'])
    return df

# parses a dataframe into a sql insert
def df_to_sql(df, table):
    cols = ', '.join(df.columns.to_list()) 
    vals = []

    for index, r in df.iterrows():
        row = []
        for x in r:
            row.append(f"'{x}'")

        row_str = ', '.join(row)
        vals.append(row_str)

    f_values = [] 
    for v in vals:
        f_values.append(f'({v})')

    # Handle inputting NULL values
    f_values = ', '.join(f_values) 
    f_values = re.sub(r"('None')", "NULL", f_values)

    sql = f"""insert into {table} ({cols}) values {f_values}"""

    return sql

# connect to db and create schema
params = {
    "user": "eva",
    "password": "eva_password",
    "host": "localhost",
    "port": "3306",
    "database": "reddit_emojis"
}
cursor.query(f"CREATE DATABASE reddit_emojis WITH ENGINE = 'mysql', PARAMETERS = {params}")
cursor.query("""
    USE reddit_emojis {
             CREATE TABLE IF NOT EXISTS submissions (
                `index` INT,
                id varchar(255),
                title TEXT,
                text TEXT,
                full_text TEXT,
                url varchar(255),
                PRIMARY KEY (id)
             )
    }
""").df()
# cursor.query("""
#         CREATE FUNCTION IF NOT EXISTS OpenAIChatCompletion
#         IMPL 'evadb/functions/openai_chat_completion_function.py'
#         MODEL 'gpt-3.5-turbo'
# """).df()

# query = df_to_sql(get_submissions(), "submissions")
# query = """
#     USE reddit_emojis {
# """ + query + """}"""
# print("QUERY: ", query)
# print(cursor.query(query).df())

# populate DB
conn = sqlalchemy.create_engine('mysql+mysqlconnector://eva:eva_password@localhost:3306/reddit_emojis')

submissions = get_submissions()
try:
    submissions.to_sql(name='submissions', con=conn, if_exists='append')
except:
    print("Duplicate entries")

gpt_completion = cursor.query("""SELECT id, ChatGPT('React using an emoji to capture the sentiment of this', full_text) FROM reddit_emojis.submissions;
                   """).df()
gpt_completion.to_sql(name='submission_emojis', con=conn, if_exists='replace')


