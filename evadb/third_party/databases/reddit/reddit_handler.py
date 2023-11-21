# coding=utf-8
# Copyright 2018-2023 EvaDB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import evadb
import pandas as pd
import praw
import re
import sqlalchemy

from evadb.third_party.databases.types import DBHandler, DBHandlerResponse, DBHandlerStatus
from evadb.third_party.databases.reddit.table_column_info import REDDIT_COLUMNS
from prawcore import ResponseException

class RedditHandler(DBHandler):
    def __init__(self, name: str, **kwargs):
        """
        Initialize Reddit Handler. Each handler represents a subreddit.
        Args:
            name (str): name of DB Handler
            **kwargs: arguments needed for Reddit API
        """
        super().__init__(name)
        self.client_id = kwargs.get("client_id")
        self.client_secret = kwargs.get("client_secret")
        self.user_agent = kwargs.get("user_agent")
        self.subreddit = kwargs.get("subreddit")
        self.num_results = kwargs.get("num_results")

    def connect(self):
        try:
            self.connection = praw.Reddit(
                client_id=self.client_id,
                client_secret=self.client_secret,
                user_agent=self.user_agent
            )
            return DBHandlerStatus(status=True)
        except Exception as e:
            return DBHandlerStatus(status=False, error=str(e))
        
    def disconnect(self):
        if self.connection:
            self.connection = None
        
    def check_connection(self):
        """
        Checks if praw client is valid.
        """
        if self.connection is None:
            return False
        
        try:
            self.connection.user.me()
        except ResponseException:
            return False
        return True
    
    def get_tables(self) -> DBHandlerResponse:
        """
        Returns the tables in the client. Each table is one of the filters
        """
        tables = [
            "top",
            "hot",
            "new",
            "rising"
        ]
        tables_df = pd.DataFrame(tables, columns=['table_name'])
        return DBHandlerResponse(data=tables_df)
    
    def get_columns(self, table_name: str) -> DBHandlerResponse:
        columns_df = pd.DataFrame(REDDIT_COLUMNS, columns=["name", "dtype"])
        return DBHandlerResponse(data=columns_df)
    
    def _get_submissions_generator(self, table_name: str) -> DBHandlerResponse:
        """
        Gets submissions from the table with the specified parameters of the current engine 
        (subreddit, num_results, etc.), and creates a generator from this data.
        Args:
            table_name (str): name of the table to get submissions from
        """
        reddit = praw.Reddit(
            client_id=self.client_id,
            client_secret=self.client_secret,
            user_agent=self.user_agent
        )

        subreddit = reddit.subreddit(self.subreddit)
        posts = None

        if table_name == 'top':
            posts = subreddit.top(time_filter="all", limit=int(self.num_results))
        elif table_name == 'hot':
            posts = subreddit.hot(limit=int(self.num_results))
        elif table_name == 'new':
            posts = subreddit.new(limit=int(self.num_results))
        elif table_name == 'rising':
            posts = subreddit.rising(limit=int(self.num_results))
        else:
            return DBHandlerResponse(data=None, error=f"{table_name} is not supported or does not exist.")
        
        # create generator. Set author to None if deleted.
        for submission in posts:
            yield {
                'id': submission.id,
                'author': submission.author.name if submission.author else None,
                'created': submission.created_utc,
                'score': submission.score,
                'title': submission.title,
                'text': submission.selftext,
                'full_text': submission.title + "\n " + submission.selftext,
                'url': submission.url
            }

    def select(self, table_name: str) -> DBHandlerResponse:
        """
        Returns a generator with data from the target table.
        Args:
            table_name (str): the target table
        """
        if not self.check_connection:
            return DBHandlerResponse(data=None, error="Not connected to the datasource.")
        
        try:
            return DBHandlerResponse(
                data=None,
                data_generator=self._get_submissions_generator(table_name)
            )
        except Exception as e:
            return DBHandlerResponse(data=None, error=str(e))
