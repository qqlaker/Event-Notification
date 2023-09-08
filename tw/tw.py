import multiprocessing
import tweepy
import time
import requests
import configparser
import os
import math
import csv
import pandas as pd
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import gspread_dataframe as gd
from tqdm import tqdm
import socket
import threading


CONFIG = configparser.RawConfigParser()
CONFIG.read('configs/config.ini')
BEARER_TOKEN = CONFIG['twitter']['bearer_token']
AUTH = tweepy.OAuth2BearerHandler(bearer_token=BEARER_TOKEN)
API = tweepy.API(AUTH, wait_on_rate_limit=True)

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

CREDS = ServiceAccountCredentials.from_json_keyfile_name("configs/credentials.json", SCOPES)
CLIENT = gspread.authorize(CREDS)


def search(following, user="None", keyword="None"):
    # ----------------------------------------------------------------------------------------------
    # user - if None, will search among all tweets from all users
    # keyword - search terms [necessary when user=None]
    # ----------------------------------------------------------------------------------------------

    if keyword != "None" and type(keyword) == str:
        keyword += f' since:{str(datetime.now().date()-timedelta(days=1))}'

    tws = []
    if user != "None":
        if keyword == "None":
            keyword = ''
        if user.lower() == '|follow|':
            follows_addition = '('
            conditions = []
            for user in following:
                if len(follows_addition) > 300:
                    follows_addition = follows_addition[:-4]
                    follows_addition += ')'
                    conditions.append(follows_addition)
                    follows_addition = '('
                follows_addition += f'from:{user} OR '
            follows_addition = follows_addition[:-4]
            follows_addition += ')'
            conditions.append(follows_addition)
            print('cond:', len(conditions))
            for addit in conditions:
                print(addit)
                que = f'{addit} AND {keyword}'
                print(que)
                tws.append(tweepy.Cursor(API.search_tweets, q=que, count=200, tweet_mode='extended').items(1000))
        else:
            tws.append(tweepy.Cursor(API.search_tweets, q=f'from:({user}) AND ({keyword})', count=200, tweet_mode='extended').items(1000))
    else:
        if keyword == "None":
            print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} | Error | Missing [keyword] and [user] arguments in search()')
            return False
        tws.append(tweepy.Cursor(API.search_tweets, q=keyword, count=200, tweet_mode='extended').items(1000))


    for tweets in tws:
        for tweet in tweets:
            yield tweet


def excel_process(new_df=None):  # if new_df did not specified, function will only remove duplicates from tables
    # -----------------------------------
    # write to csv and google sheets
    # -----------------------------------

    sheet = CLIENT.open(CONFIG['Tables']['google_sheets_table_name']).sheet1


    def sort_df(df, column_idx):
        cmp = lambda x: 1 if 'sent' in x else 0
        col = df.loc[:, column_idx]
        df = df.loc[[i[1] for i in sorted(zip(col, range(len(col))), key=cmp)]]
        return df


    def func2(df1):
        df1 = sort_df(df1, 'Discord').drop_duplicates('Tweet', keep="last")
        df1 = df1.sort_values(by='Date', ascending=False)
        df1.to_csv(CONFIG['Tables']['excel_table_name'], index=False)
        sheet.clear()
        gd.set_with_dataframe(sheet, df1)

    df = pd.read_csv(CONFIG['Tables']['excel_table_name'])

    if df.empty:
        if new_df is not None:
            func2(new_df)
            return True
        else:
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Warning | {CONFIG['Tables']['excel_table_name']} file is empty")
            return False

    if new_df is not None:
        if type(new_df) is pd.DataFrame:
            df = pd.concat([df, new_df], ignore_index=True)
        else:
            print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} | Warning | [new_df] you try to concatenate is not DataFrame. Script will update existing table only.')
    func2(df)
    return True


def discord_process(following):

    channel_id_following = CONFIG['Discord']['channel_id_following']
    channel_id_all = CONFIG['Discord']['channel_id_all']

    df = pd.read_csv(CONFIG['Tables']['excel_table_name'])
    index = 0
    for row in df.itertuples(index=False, name='Tweet'):
        if type(row[3]) == float:
            user = row[1]
            msg = f'---------------------------------\n{row[1]} | {row[0]}\n\n{row[4]}'  # message style
            if user in following:
                pass
                #print(msg)
                #result = sendMessage(channel_id=channel_id_following, message=msg)
            else:
                pass
                #print(msg)
                #result = sendMessage(channel_id=channel_id_all, message=msg)
            result = True  # TODO: УБРАТЬ!
            if result:
                df.loc[index, 'Discord'] = 'sent'
                df.to_csv(CONFIG['Tables']['excel_table_name'], index=False)
                #print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} | user: {row[1]} | message date: {row[0]} | sent')
        index += 1
        if index % 1000 == 0:
            sheet = CLIENT.open(CONFIG['Tables']['google_sheets_table_name']).sheet1
            sheet.clear()
            gd.set_with_dataframe(sheet, df)
    sheet = CLIENT.open(CONFIG['Tables']['google_sheets_table_name']).sheet1
    sheet.clear()
    gd.set_with_dataframe(sheet, df)
    excel_process()


def sendMessage(channel_id, message):
    url = 'https://discord.com/api/v8/channels/{}/messages'.format(channel_id)
    data = {"content": message}
    header = {"authorization": CONFIG['Discord']['token']}
    while True:
        r = requests.post(url, data=data, headers=header)
        if r.status_code == 200:
            return True
        elif r.status_code == 429:
            time.sleep(15)
            print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} | Await | Discord request limit exceeded')
        else:
            print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} | Error | An error occurred while requesting Discord | status_code: {r.status_code}')
            return False


def clearSavedCsv():
    files = os.listdir('saved_csv')
    #for i in range(len(files)):
    #    files[i] = datetime.datetime.fromtimestamp(os.path.getctime(f"saved_csv/{files[i]}")).strftime('%Y-%m-%d %H.%M.%S %p.csv')
    files = sorted(files)
    if len(files) > 300:
        for i in range(len(files)-300):
            os.remove(f"saved_csv/{files[i]}")


def main():
    def status():
        msg = 'Twitter BOT'
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect(('localhost', 8888))
        except ConnectionRefusedError:
            print(
                f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Status server is not working. Please start server.py before bots.")
            return
        while 1:
            rec_msg = s.recv(1024).decode('utf-8')
            s.send(msg.encode('utf-8'))
        s.close()

    status_twitter = threading.Thread(target=status, daemon=True)
    status_twitter.start()

    if not os.path.exists(CONFIG['Tables']['excel_table_name']):
        with open(CONFIG['Tables']['excel_table_name'], 'w') as f:
            writer = csv.writer(f)
            columns = ["Date", "User", "Tweet", "Discord", "Url"]
            writer.writerow(columns)
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {CONFIG['Tables']['excel_table_name']} created")

    df = pd.read_csv(CONFIG['Tables']['excel_table_name'])

    if not df.empty:
        excel_process()

    settings = []
    with open('configs/searching.txt', 'r') as f:
        for line in f:
            li = line.strip('\n').split(' ')
            settings.append(li)
    for i in range(len(settings)):
        for j in range(len(settings[i])):
            settings[i][j] = settings[i][j].strip(',').split('=')[1]
            if j == 1:
                settings[i][j] = settings[i][j].replace('_', ' ')

    following = []
    for user in tweepy.Cursor(API.get_friends, count=200, screen_name=CONFIG['twitter']['your_twitter_name']).items():
        following.append(user.screen_name)
    #with open('foll.txt', 'r') as f:
    #    for line in f:
    #        following.append(line.strip('\n'))
    while True:
        dataframes = []
        data = []
        count = 0
        for s in tqdm(settings):
            tweets_gen = search(following=following, user=s[0], keyword=s[1])
            for tweet in tweets_gen:
                count += 1
                screen_name = tweet.user.screen_name
                tweet_id = tweet.id
                url = f'https://twitter.com/{screen_name}/status/{tweet_id}'
                data.append([str(tweet.created_at.replace(tzinfo=None)), screen_name, tweet.full_text, math.nan, url])
            df = pd.DataFrame(data, columns=["Date", "User", "Tweet", "Discord", "Url"])
            if type(df) is pd.DataFrame:
               dataframes.append(df)
        for _ in range(len(dataframes)):
            excel_process(dataframes.pop(0))

        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} | comparing ...')
        discord_process(following)

        print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} | sleeping for 300')
        for _ in tqdm(range(300)):
            time.sleep(1)


if __name__ == '__main__':
    while True:
        proc = multiprocessing.Process(target=main)
        proc.start()
        time.sleep(43200)
        proc.terminate()
        clearSavedCsv()
        time.sleep(300)
