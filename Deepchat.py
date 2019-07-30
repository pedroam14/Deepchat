import sqlite3
import json
from datetime import datetime

timeframe = 'Data/2017-10'  # location to json dataset
sql_transaction = []

connection = sqlite3.connect('{}.db'.format(timeframe))
c = connection.cursor()


def create_table():
    #c.execute("CREATE TABLE IF NOT EXISTS parent_reply(parent_id TEXT PRIMARY KEY, comment_id TEXT UNIQUE, parent TEXT, comment TEXT, subreddit TEXT, unix INT, score INT)")
    print("teste :||||")

if __name__ == '__main__':
    #create_table()
    print("teste :DDD")