import requests, json, os, time
import pandas as pd
import configparser
import math
import gspread
import csv
from oauth2client.service_account import ServiceAccountCredentials
import gspread_dataframe as gd
from currency_converter import CurrencyConverter
import pytz, dateutil.parser
from datetime import timedelta
import datetime
import socket
import threading
import multiprocessing
import colorama
from colorama import Fore, Style


CONFIG = configparser.RawConfigParser()
CONFIG.read('configs/config.ini')
API_KEYS = CONFIG['Ticketmaster']['ConsumerKey'].strip('\n').split(',')


def sendMessage(channel_id, message, headers):
    url = 'https://discord.com/api/v8/channels/{}/messages'.format(channel_id)
    data = {"content": message}
    while True:
        r = requests.post(url, data=data, headers=headers)
        if r.status_code == 200:
            return True
        elif r.status_code == 429:
            time.sleep(10)
            print(f'{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")} | Sending...')
        else:
            print(f'{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")} | Error | An error occurred while requesting Discord | status_code: {r.status_code}')
            return False


def search(artist, api_key):
    def check_exist(elem, area, c=1):
        if elem in area:
            return area[elem]
        else:
            if c == 0:
                return 0
            if c == 2:
                return ''
        return math.nan

    def check_date(elem, area):
        if elem in area:
            a = check_exist(elem, area)
            if a != math.nan:
                utctime = dateutil.parser.parse(a)
            if timezonez != math.nan:
                utctime = utctime.astimezone(pytz.timezone('America/Los_Angeles')).replace(tzinfo=None)
                if utctime.year <= 1950:
                    utctime += timedelta(days=36500)
                return str(utctime)
            else:
                utctime = utctime.replace(tzinfo=None)
                if utctime.year <= 1950:
                    utctime += timedelta(days=36500)
                return str(utctime)
        elif 'localDate' in area:
            return str(check_exist('localDate', area))
        return math.nan

    events_for_artist = json.loads(requests.get(
        f'https://app.ticketmaster.com/discovery/v2/events?attractionId={artist}&locale=*&size=200&includeTBA=yes&includeTBD=yes&apikey={api_key}').text)
    ev = check_exist(elem='_embedded', area=events_for_artist, c=0)
    if ev == 0:
        if check_exist(elem='_links', area=events_for_artist, c=0) == 0:
            ft = check_exist(elem='fault', area=events_for_artist, c=0)
            if ft:
                if 'Rate limit' in ft['faultstring']:
                    return [False, 'rate']
            return [False, False]

    data = []
    if ev:
        for i in range(len(check_exist('events', ev, c=2))):
            timezonez = check_exist('timezone', check_exist('dates', ev['events'][i]))

            name = ev['events'][i]['name']
            id = ev['events'][i]['id']

            start_datetime = math.nan
            TBA = math.nan
            TBD = math.nan
            start = check_exist('start', ev['events'][i]['dates'])
            if type(start) != float:
                start_datetime = check_date('dateTime', start)
                TBA = check_exist('dateTBA', ev['events'][i]['dates']['start'])
                TBD = check_exist('dateTBD', ev['events'][i]['dates']['start'])

            venue_names = []
            for k in range(len(check_exist('venues', ev['events'][i]['_embedded'], 0))):
                venue_names.append(check_exist('name', ev['events'][i]['_embedded']['venues'][k]))
            while math.nan in venue_names:
                venue_names.remove(math.nan)
            if not venue_names:
                venue_names = math.nan
            else:
                s = ''
                for el in venue_names:
                    s += f'{el}, '
                venue_names = s[:-2]

            attractions_names = []
            for k in range(len(check_exist('attractions', ev['events'][i]['_embedded'], 0))):
                attractions_names.append(check_exist('name', ev['events'][i]['_embedded']['attractions'][k]))
            while math.nan in attractions_names:
                attractions_names.remove(math.nan)
            if not attractions_names:
                attractions_names = math.nan
            else:
                s = ''
                for el in attractions_names:
                    s += f'{el}, '
                attractions_names = s[:-2]

            if 'public' in ev['events'][i]['sales']:
                public_start_datetime = check_date('startDateTime', ev['events'][i]['sales']['public'])
                public_end_datetime = check_date('endDateTime', ev['events'][i]['sales']['public'])
            else:
                public_start_datetime = math.nan
                public_end_datetime = math.nan

            presales_start_datetime = []
            presales_end_datetime = []
            if 'presales' in ev['events'][i]['sales']:
                for k in range(len(ev['events'][i]['sales']['presales'])):
                    presales_start_datetime.append(check_date('startDateTime', ev['events'][i]['sales']['presales'][k]))
                for k in range(len(ev['events'][i]['sales']['presales'])):
                    presales_end_datetime.append(check_date('endDateTime', ev['events'][i]['sales']['presales'][k]))
            while math.nan in presales_start_datetime:
                presales_start_datetime.remove(math.nan)
            while math.nan in presales_end_datetime:
                presales_end_datetime.remove(math.nan)
            if not presales_start_datetime:
                presales_start_datetime = math.nan
            else:
                s = ''
                for el in presales_start_datetime:
                    s += f'{el}, '
                presales_start_datetime = s[:-2]
            if not presales_end_datetime:
                presales_end_datetime = math.nan
            else:
                s = ''
                for el in presales_end_datetime:
                    s += f'{el}, '
                presales_end_datetime = s[:-2]

            url = check_exist('url', ev['events'][i])

            priceRanges = {}
            if 'priceRanges' in ev['events'][i]:
                for k in range(len(ev['events'][i]['priceRanges'])):
                    if ev['events'][i]['priceRanges'][k]['type'] == 'standard':
                        priceRanges = {'min': check_exist('min', ev['events'][i]['priceRanges'][k]),
                                                'max': check_exist('max', ev['events'][i]['priceRanges'][k])}
                        currency = check_exist('currency', ev['events'][i]['priceRanges'][k])
                s = ''
                try:
                    if currency == "USD":
                        s += f'min: {round(float(priceRanges["min"]), 2)} USD, max: {round(float(priceRanges["max"]), 2)} USD, '
                    else:
                        c = CurrencyConverter()
                        s += f'min: {round(c.convert(float(priceRanges["min"]), currency, "USD"), 2)} USD, max: {round(c.convert(float(priceRanges["max"]), currency, "USD"), 2)} USD, '
                except ValueError:
                    s += f'min: {round(float(priceRanges["min"]), 2)} {currency}, max: {round(float(priceRanges["max"]), 2)} {currency}, '
                priceRanges = s[:-2]
            if priceRanges == {}:
                priceRanges = math.nan

            ticketLimit = check_exist('info', check_exist('ticketLimit', ev['events'][i], 2))

            data.append([name, start_datetime, venue_names, attractions_names, public_start_datetime,
                public_end_datetime, presales_start_datetime, presales_end_datetime, url, artist, id, priceRanges, ticketLimit, timezonez, TBA, TBD])

    columns = ['name', 'start datetime', 'venue names', 'attractions names', 'public start datetime', 'public end datetime',
               'presales start datetime', 'presales end datetime', 'url', 'artist', 'id', 'price ranges', 'ticket limit', 'timezone', 'TBA', 'TBD']
    df = pd.DataFrame(data, columns=columns)
    return [df, True, ev]


def compare_id(df_new):
    channel_id_new_events = CONFIG['Discord']['channel_id_new_events'].strip('\n').split(',')
    channel_id_updates = CONFIG['Discord']['channel_id_updates'].strip('\n').split(',')
    headers = CONFIG['Discord']['token'].strip('\n').split(',')
    df_excel = pd.read_csv(CONFIG['Tables']['excel_table_name'])

    def compare_row(index_excel, index_new, df_new):
        changes = []
        for column in df_new:
            if column != 'artist':
                cur = [column]
                first = df_new.loc[index_new][column]
                second = df_excel.loc[index_excel][column]
                if first != second:
                    if not(str(first).lower() == 'nan' and str(second).lower() == 'nan'):
                        if not(str(first).lower() == 'nat' and str(second).lower() == 'nat'):
                            cur.append(second)
                            cur.append(first)
                            changes.append(cur)
        return changes

    if df_excel.empty:
        file_list = os.listdir('saved_csv')
        if len(file_list) < 1:
            columns = ['name', 'start datetime', 'venue names', 'attractions names', 'public start datetime',
                       'public end datetime',
                       'presales start datetime', 'presales end datetime', 'url', 'artist', 'id', 'price ranges',
                       'ticket limit', 'timezone', 'TBA', 'TBD']
            df_excel = pd.DataFrame(columns=columns)
        else:
            full_list = [os.path.join('saved_csv', i) for i in file_list]
            time_sorted_list = sorted(full_list, key=os.path.getmtime)
            df_excel = pd.read_csv(time_sorted_list[-1])

    if 'Unnamed: 0' in df_excel.columns:
        del df_excel['Unnamed: 0']

    ids_excel = df_excel['id'].to_list()
    ids_new = df_new['id'].to_list()
    for id_new in ids_new:
        new = df_new.loc[df_new['id'] == f'{id_new}'].index[0]
        if id_new in ids_excel:
            old = df_excel.loc[df_excel['id'] == f'{id_new}'].index[0]
            changes = compare_row(index_excel=old, index_new=new, df_new=df_new)
            if changes:
                f = False
                for i in range(len(changes)):
                    if ('presales' or 'public') in changes[i][0]:
                        f = True
                if f:
                    msg = f'----------------@everyone------------------\n'
                else:
                    msg = f'-------------------------------------------\n'
                msg += f'{df_new.loc[new]["name"]}\n'
                for i in range(len(changes)):
                    msg += f'• {changes[i][0]}: {changes[i][1]} --> {changes[i][2]}\n'
                msg += f'\n• Artist ID: {df_new.loc[new]["artist"]} | Event ID: {df_new.loc[new]["id"]}\n' \
                       f'• Venues: {df_new.loc[new]["venue names"]}\n' \
                       f'{df_new.loc[new]["url"]}\n'
                for n in range(len(channel_id_updates)):
                    hrs = {"authorization": headers[n]}
                    sendMessage(channel_id=channel_id_updates[n], message=msg, headers=hrs)
                df_excel.loc[old] = df_new.loc[new]
        else:
            cur_row = df_new.loc[df_new['id'] == f'{id_new}']
            df_excel = pd.concat([df_excel, cur_row], axis=0, ignore_index=True)
            df_excel.drop_duplicates(subset="id", keep='last', inplace=True)
            if 'Unnamed: 0' in df_new.columns:
                del df_new['Unnamed: 0']
            msg = f'----------------@everyone------------------\n' \
                  f'{df_new.loc[new]["name"]}\n• Start datetime: {df_new.loc[new]["start datetime"]}\n' \
                  f'• Venues: {df_new.loc[new]["venue names"]}\n• Attractions: {df_new.loc[new]["attractions names"]}\n' \
                  f'• Public sales start datetime: {df_new.loc[new]["public start datetime"]}\n' \
                  f'• Public sales end datetime: {df_new.loc[new]["public end datetime"]}\n' \
                  f'• Presales start datetime: {df_new.loc[new]["presales start datetime"]}\n' \
                  f'• Presales end datetime: {df_new.loc[new]["presales end datetime"]}\n' \
                  f'• Price range: {df_new.loc[new]["price ranges"]}\n' \
                  f'• Ticket limit: {df_new.loc[new]["ticket limit"]}\n' \
                  f'• Timezone: {df_new.loc[new]["timezone"]}\n' \
                  f'• TBA: {df_new.loc[new]["TBA"]} | TBD: {df_new.loc[new]["TBD"]}\n\n' \
                  f'• Artist ID: {df_new.loc[new]["artist"]} | Event ID: {df_new.loc[new]["id"]}\n' \
                  f'{df_new.loc[new]["url"]}\n'
            for n in range(len(channel_id_new_events)):
                hrs = {"authorization": headers[n]}
                sendMessage(channel_id=channel_id_new_events[n], message=msg, headers=hrs)
    if 'Unnamed: 0' in df_excel.columns:
        del df_excel['Unnamed: 0']
    df_excel.to_csv(f"saved_csv/{datetime.datetime.now().strftime('%Y-%m-%d %H.%M.%S')}.csv", index=False)
    df_excel.to_csv(CONFIG['Tables']['excel_table_name'], index=False)
    # TODO: TO CSV --> read from txt --> sendMessage --> clear txt

    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name("configs/credentials.json", scopes)
    client = gspread.authorize(creds)
    sheet = client.open(CONFIG['Tables']['google_sheets_table_name']).sheet1
    sheet.clear()
    gd.set_with_dataframe(sheet, df_new)


def clearSavedCsv():
    files = os.listdir('saved_csv')
    for i in range(len(files)):
        files[i] = datetime.datetime.fromtimestamp(os.path.getctime(f"saved_csv/{files[i]}")).strftime('%Y-%m-%d %H.%M.%S.csv')
    files = sorted(files)
    if len(files) > 10:
        for i in range(len(files)-10):
            os.remove(f"saved_csv/{files[i]}")


def main():
    def status():
        msg = 'TicketMaster BOT'
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect(('localhost', 8888))
        except ConnectionRefusedError:
            print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Status server is not working. Please start server.py before bots.")
            return
        while 1:
            rec_msg = s.recv(1024).decode('utf-8')
            s.send(msg.encode('utf-8'))
        s.close()

    status_tm = threading.Thread(target=status, daemon=True)
    status_tm.start()

    artists = []
    artists1 = []
    with open('configs/artists.txt', 'r') as f:
        for line in f:
            artists1.append(line.strip('\n'))
    for item in artists1:
        if item not in artists:
            artists.append(item)

    if not os.path.exists(CONFIG['Tables']['excel_table_name']):
        with open(CONFIG['Tables']['excel_table_name'], 'w') as f:
            writer = csv.writer(f)
            columns = ['name', 'start datetime', 'venue names', 'attractions names', 'public start datetime',
                       'public end datetime',
                       'presales start datetime', 'presales end datetime', 'url', 'artist', 'id', 'price ranges',
                       'ticket limit', 'timezone', 'TBA', 'TBD']
            writer.writerow(columns)
            print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {CONFIG['Tables']['excel_table_name']} created")

    rate = (86400 // ((len(API_KEYS) * 5000) // len(artists)))  # rate limit = 5000 per day
    artists = []

    def change_api_key():
        def check_exist(elem, area, c=1):
            if elem in area:
                return area[elem]
            else:
                if c == 0:
                    return 0
                if c == 2:
                    return ''
            return math.nan
        b = 1
        while b != len(API_KEYS):
            api_key = API_KEYS.pop(0)
            API_KEYS.append(api_key)
            b += 1
            r = json.loads(requests.get(
                f'https://app.ticketmaster.com/discovery/v2/events.json?attractionId=K8vZ917KWbV&includeTBA=yes&includeTBD=yes&size=100&apikey={api_key}').text)
            if check_exist("fault", r) != math.nan:
                return True
        print(f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Rate limit exceeded. Not enough api")

    while True:
        for item in artists1:
            if item not in artists:
                artists.append(item)
        print(f'{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")} | searching ...')
        dfs = []
        evs = []
        while len(artists) != 0:
            artist = artists[0]
            df = search(artist, API_KEYS[0])
            if df[1] == 'rate':
                change_api_key()
            elif df[1] == True:
                dfs.append(df[0])
                evs.append(df[2])
                artists.remove(artist)
        df_new = pd.concat(dfs, axis=0, ignore_index=True)
        #df_new.reset_index(drop=True, inplace=True)
        df_new.drop_duplicates(subset="id", keep='last', inplace=True)
        if 'Unnamed: 0' in df_new.columns:
            del df_new['Unnamed: 0']
        data = json.dumps(evs, indent=2)
        with open(f"saved_csv/{datetime.datetime.now().strftime('%Y-%m-%d %H.%M.%S')} data.txt", "w+") as f:
            f.write(data)
        print(f'{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")} | comparing ...')
        compare_id(df_new)

        print(f'{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")} | sleeping ---')
        time.sleep(rate)


if __name__ == '__main__':
    os.system("cls")
    print(Fore.GREEN + Style.BRIGHT + '----------------------------------------------\nTM-Notification | telegram/github --> @qqlaker \n')
    print(Style.RESET_ALL)
    while True:
        proc = multiprocessing.Process(target=main)
        proc.start()
        for t in range(36000):
            time.sleep(1)
            if not proc.is_alive():
                break
        proc.terminate()
        clearSavedCsv()
        time.sleep(60)
