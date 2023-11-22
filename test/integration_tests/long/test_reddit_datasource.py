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
import unittest
from test.util import get_evadb_for_testing

import pytest

from evadb.server.command_handler import execute_query_fetch_all
from evadb.third_party.databases.reddit.table_column_info import REDDIT_COLUMNS


@pytest.mark.notparallel
class RedditDataSourceTest(unittest.TestCase):
    def setUp(self):
        self.evadb = get_evadb_for_testing()
        # reset catalog manager
        self.evadb.catalog().reset()

    def tearDown(self):
        execute_query_fetch_all(self.evadb, "DROP DATABASE IF EXISTS reddit_data")
    
    @pytest.mark.xfail(reason="Need to provide API credentials in params to run test case")
    def test_should_run_select_query_in_reddit(self):
        # Create database.
        params = {
            "client_id": "<CLIENT_ID>",
            "client_secret": "<CLIENT_SECRET>",
            "user_agent": "<USER_AGENT>",
            "subreddit": "gatech",
            "num_results": "10"
        }
        print("params done")
        query = f"""CREATE DATABASE reddit_data
                    WITH ENGINE = "reddit",
                    PARAMETERS = {params};"""
        execute_query_fetch_all(self.evadb, query)
        print("query done")

        query = "SELECT * FROM reddit_data.top;"
        batch = execute_query_fetch_all(self.evadb, query)
        self.assertEqual(len(batch), 10)
        expected_column = list(
            ["top.{}".format(col) for col, _ in REDDIT_COLUMNS]
        )
        self.assertEqual(batch.columns, expected_column)