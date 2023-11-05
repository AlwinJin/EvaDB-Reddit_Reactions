### Overview
This project uses EvaDB to generate emoji reactions for posts in a subreddit. It makes use of the praw module, a thin wrapper over the Reddit API, to scrape data from Reddit. It then feeds this data into GPT-3.5 to generate emoji reactions for various posts.

### Usage
Install the dependencies: \
`pip install -r requirements.txt` \

Then run the program: \
`python main.py` \

Subreddit choice, filter choice, and number of results can be adjusted by changing their respective paremeters in `main.py
`