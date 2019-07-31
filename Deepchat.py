import sqlite3
import json
from datetime import datetime

timeframe = 'Data/2017-10'  # location to json dataset
sqlTransaction = []

dataFilePath = 'Data/2017-10'


connection = sqlite3.connect('{}.db'.format(timeframe))
c = connection.cursor()


def CreateTable():
    c.execute("CREATE TABLE IF NOT EXISTS parent_reply(parent_id TEXT PRIMARY KEY, comment_id TEXT UNIQUE, parent TEXT, comment TEXT, subreddit TEXT, unix INT, score INT)")
    # print("teste :||||")
    # print('{} teste'.format(timeframe))


def FormatData(data):
    data = data.replace("\n", " newlinechar ").replace(
        "\r", " newlinechar ").replace('"', "'")
    return data


def Acceptable(data):
    if len(data.split(' ')) > 50 or len(data) < 1:
        return False
    elif len(data) > 1000:
        return False
    elif data == '[deleted]' or data == '[removed]':
        return False
    else:
        return True


def FindParent(pid):
    try:
        sql = "SELECT comment FROM parent_reply WHERE comment_id = '{}' LIMIT 1".format(
            pid)
        c.execute(sql)
        result = c.fetchone()
        if result != None:
            return result[0]
        else:
            return False
    except Exception as e:
        # print(str(e))
        return False


def FindExistingScore(pid):
    try:
        sql = "SELECT score FROM parent_reply WHERE parent_id = '{}' LIMIT 1".format(
            pid)
        c.execute(sql)
        result = c.fetchone()
        if (result != None):
            return result[0]
        else:
            return False
    except Exception as e:
        # print(str(e))
        return False


if __name__ == '__main__':
    CreateTable()
    rowCounter = 0
    pairedRows = 0
    # open defaults to 'r' but still, y'know
    with open(dataFilePath, mode='r', buffering=1000) as f:
        for row in f:
            rowCounter += 1
            row = json.loads(row)
            parentID = row['parent_id']
            body = FormatData(row['body'])
            createdUTC = row['created_utc']
            score = row['score']
            commentID = row['name']
            subreddit = row['subreddit']
            parentData = FindParent(parentID)
            if(score >= 10):
                if(Acceptable(body)):
                    existingCommentScore = FindExistingScore(parentID)
                    if (existingCommentScore):
                        if (score > existingCommentScore):
                            SQLInsertReplaceComment(commentID, parentID)

                    else:
                        if (parentData):
                            SQLInsertHasParent()
                        else:
                            SQLInsertNoParent()
                        # existing_comment_score = FindExistingScore(parentID)
                        # print("teste :DDD")
