### Overview
This project uses EvaDB to generate emoji reactions for posts in a subreddit. It makes use of the praw module, a thin wrapper over the Reddit API, to scrape data from Reddit. It then feeds this data into GPT-3.5 to generate emoji reactions for various posts. This project also contributes a new Reddit data source to EvaDB itself.

### Usage
Install the dependencies:

`pip install -r requirements.txt` 

Then run the program:

`python main.py` 

To connect to Reddit as a data source:
```
params = {
    "client_id": "<CLIENT_ID>",
    "client_secret": "<CLIENT_SECRET>",
    "user_agent": "<USER_AGENT>",
    "subreddit": "gatech",
    "num_results": "10"
}
cursor.query("DROP DATABASE IF EXISTS reddit_data").df()
cursor.query(f"CREATE DATABASE reddit_data WITH ENGINE = 'reddit', PARAMETERS = {params}").df()
```
