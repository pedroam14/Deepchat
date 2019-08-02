import sqlite3
import json
from datetime import datetime

timeframe: str = 'Data/2017-10'  # location to json dataset
sqlTransaction: list = []

dataFilePath: str = 'Data/2017-10'

startRow = 0
cleanup = 1000000

connection: sqlite3.Connection = sqlite3.connect('{}.db'.format(timeframe))
c: sqlite3.Cursor = connection.cursor()


def CreateTable():
    c.execute("CREATE TABLE IF NOT EXISTS parent_reply(parentID TEXT PRIMARY KEY, commentID TEXT UNIQUE, parent TEXT, comment TEXT, subreddit TEXT, unix INT, score INT)")
    # print("teste :||||")
    # print('{} teste'.format(timeframe))


def FormatData(data: str):
    data = data.replace("\n", " newlinechar ").replace(
        "\r", " newlinechar ").replace('"', "'")
    return data


def Acceptable(data: str):
    if (len(data.split(' ')) > 50) or (len(data) < 1):
        return False
    elif (len(data) > 1000):
        return False
    elif data == '[deleted]' or data == '[removed]':
        return False
    else:
        return True


def FindParent(pid: str):
    try:
        sql = "SELECT comment FROM parent_reply WHERE commentID = '{}' LIMIT 1".format(
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


def FindExistingScore(pid: str):
    try:
        sql = "SELECT score FROM parent_reply WHERE parentID = '{}' LIMIT 1".format(
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


def TransactionBuilder(sql: str):
    global sqlTransaction
    sqlTransaction.append(sql)
    if (len(sqlTransaction) > 1000):
        c.execute('BEGIN TRANSACTION')
        for s in sqlTransaction:
            try:
                c.execute(s)
            except:
                pass
        connection.commit()
        sqlTransaction = []


def SQLInsertReplaceComment(commentid: str, parentid: str, parent: bool, comment: str, subreddit: str, time: int, score: int):
    try:
        sql = """UPDATE parent_reply SET parent_id = ?, comment_id = ?, parent = ?, comment = ?, subreddit = ?, unix = ?, score = ? WHERE parent_id =?;""".format(
            parentid, commentid, parent, comment, subreddit, int(time), score, parentid)
        TransactionBuilder(sql)
    except Exception as e:
        print('s0 insertion', str(e))


def SQLInsertHasParent(commentid: str, parentid: str, parent: bool, comment: str, subreddit: str, time: int, score: int):
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, parent, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}","{}",{},{});""".format(
            parentid, commentid, parent, comment, subreddit, int(time), score)
        TransactionBuilder(sql)
    except Exception as e:
        print('s0 insertion', str(e))


def SQLInsertNoParent(commentid: str, parentid: str, comment: str, subreddit: str, time: int, score: int):
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}",{},{});""".format(
            parentid, commentid, comment, subreddit, int(time), score)
        TransactionBuilder(sql)
    except Exception as e:
        print('s0 insertion', str(e))


if __name__ == '__main__':
    CreateTable()
    rowCounter: int = 0
    pairedRows: int = 0

    # with open('J:/chatdata/reddit_data/{}/RC_{}'.format(timeframe.split('-')[0],timeframe), buffering=1000) as f:
    with open('Data/2017-10', buffering=1000) as f:
        for row in f:
            # print(row)
            # time.sleep(555)
            rowCounter += 1

            if rowCounter > startRow:
                try:
                    row: str = json.loads(row)
                    parentID: str = row['parent_id'].split('_')[1]
                    body: str = FormatData(row['body'])
                    createdUTC: int = row['created_utc']
                    score: int = row['score']

                    commentID: str = row['id']

                    subreddit: str = row['subreddit']
                    parentData: bool = FindParent(parentID)

                    existing_comment_score = FindExistingScore(parentID)
                    if existing_comment_score:
                        if score > existing_comment_score:
                            if Acceptable(body):
                                SQLInsertReplaceComment(
                                    commentID, parentID, parentData, body, subreddit, createdUTC, score)

                    else:
                        if Acceptable(body):
                            if parentData:
                                if score >= 20:
                                    SQLInsertHasParent(
                                        commentID, parentID, parentData, body, subreddit, createdUTC, score)
                                    pairedRows += 1
                            else:
                                SQLInsertNoParent(
                                    commentID, parentID, body, subreddit, createdUTC, score)
                except Exception as e:
                    print(str(e))

            if rowCounter % 100000 == 0:
                print('Total Rows Read: {}, Paired Rows: {}, Time: {}'.format(
                    rowCounter, pairedRows, str(datetime.now())))

            if rowCounter > startRow:
                if rowCounter % cleanup == 0:
                    print("Cleaning up!")
                    sql = "DELETE FROM parent_reply WHERE parent IS NULL"
                    c.execute(sql)
                    connection.commit()
                    c.execute("VACUUM")
                    connection.commit()
