import socket
import threading
import time
import requests
from datetime import datetime
import configparser


CONFIG = configparser.RawConfigParser()
CONFIG.read('configs/config.ini')
CHANNEL_ID_STATUS = CONFIG['Discord']['channel_id_status']
SERVICE = {}


def sendMessage(message):
    url = 'https://discord.com/api/v8/channels/{}/messages'.format(CHANNEL_ID_STATUS)
    data = {"content": message}
    header = {"authorization": CONFIG['Discord']['token']}
    while True:
        r = requests.post(url, data=data, headers=header)
        if r.status_code == 200:
            return True
        elif r.status_code == 429:
            print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} | Await | Discord request limit exceeded')
            time.sleep(15)
        else:
            print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} | Error | An error occurred while requesting Discord | status_code: {r.status_code}')
            return False


def handler(client):
    while 1:
        try:
            client.send('check'.encode('utf-8'))
            client.recv(1024).decode('utf-8')
        except ConnectionResetError:
            msg = f"{SERVICE[client]} | status: terminated"
            print(msg)
            sendMessage(msg)
            break
        time.sleep(5)
    client.close()


def main():
    print('... Waiting for connections ...')
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('localhost', 8888))
    s.listen(5)
    i = 0
    threads = {}
    while True:
        client, addr = s.accept()
        client.send('check'.encode('utf-8'))
        result_output = client.recv(1024).decode('utf-8')
        if result_output == 'Twitter BOT':
            SERVICE[client] = 'Twitter BOT'
        elif result_output == 'TicketMaster BOT':
            SERVICE[client] = 'TicketMaster BOT'
        msg = f"{SERVICE[client]} | status: started"
        print(msg)
        sendMessage(msg)
        threads[i] = threading.Thread(target=handler, args=(client,))
        threads[i].start()
        i += 1


if __name__ == '__main__':
    main()
