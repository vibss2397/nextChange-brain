import asyncio
import websockets
import ssl
import json
import time
import gzip
import base64
import time
bibox_json = {"event": "addChannel","channel": "bibox_sub_spot_BTC_USDT_depth"}
lbank_json =     {
        "action":"subscribe",
        "subscribe":"depth",
        "depth":"5",
        "pair":"btc_usdt"
    }
mutex_lock = 0
temp_json = {}
t1 = time.time()

async def send_to_server(obj, host):
    uri = "ws://localhost:8765/trade?ex=binance&pair=btc-usdt"
    async with websockets.connect(uri) as websocket:
        try:
            await websocket.send(json.dumps(obj))
        except websockets.exceptions.ConnectionClosed:
            get_data(host)

def initialize_object(values):
    global temp_json
    temp_json['binance'] = {}
    temp_json['binance']['open'] = values[0]
    temp_json['bibox'] = {}
    temp_json['bibox']['open'] = values[1]
    temp_json['lbank'] = {}
    temp_json['lbank']['open'] = values[2]

def update_json(message, host):
    global temp_json, mutex_lock, t1
    if(host.find('binance')>=0):
        host_temp = 'binance'
        last_traded_price = (float(message['bids'][len(message['bids'])-1][0]) + float(message['asks'][len(message['asks'])-1][0])) /2
    elif(host.find('bibox')>=0):
        host_temp = 'bibox'
        last_traded_price = (float(message['bids'][len(message['bids'])-1]['price']) + float(message['asks'][len(message['asks'])-1]['price'])) /2
    elif(host.find('lbk')>=0):
        host_temp = 'lbank'
        last_traded_price = (float(message['depth']['bids'][len(message['depth']['bids'])-1][0]) + float(message['depth']['asks'][len(message['depth']['asks'])-1][0])) /2

    temp_json[host_temp]['last_traded'] = last_traded_price
    #set high value
    if(not 'high' in temp_json[host_temp]):
        temp_json[host_temp]['high'] = last_traded_price
    elif(last_traded_price>temp_json[host_temp]['high']):
        temp_json[host_temp]['high'] = last_traded_price
    #set low value
    if(not 'low' in temp_json[host_temp]):
        temp_json[host_temp]['low'] = last_traded_price
    elif(last_traded_price<temp_json[host_temp]['low']):
        temp_json[host_temp]['low'] = last_traded_price

    new_json = {}
    new_json['host'] = host_temp
    new_json['time'] = time.time()
    new_json['last_traded'] = temp_json[host_temp]['last_traded']
    return new_json

async def get_data(host, ):
    global mutex_lock, temp_json
    global bibox_json,lbank_json
    initialize_object([0, 0, 0])
    temp_arr = []
    async with websockets.connect(host, ssl=ssl.SSLContext()) as websocket:
        if(host.find('bibox')>0):
            try:
                await websocket.send(json.dumps(bibox_json))
            except websockets.exceptions.ConnectionClosed:
                get_data(host)
        elif(host.find('lbk')>0):
            try:
                await websocket.send(json.dumps(lbank_json))
            except websockets.exceptions.ConnectionClosed:
                get_data(host)

        while True:
            try:
                message = await websocket.recv()
            except websockets.exceptions.ConnectionClosed:
                get_data(host)
            print(host)
            if('ping' in message):
                if(host.find('bibox')>0):
                    print('ping'+host+str(json.loads(message)))
                    try: 
                        await websocket.send(json.dumps({'pong':json.loads(message)['ping']}))
                    except websockets.exceptions.ConnectionClosed:
                        get_data(host)

                    print('done'+host)
                elif(host.find('lbk')>0):
                    print('ping'+host+str(json.loads(message)))
                    try:
                        await websocket.send(json.dumps({'action':'pong', 'pong':json.loads(message)['ping']}))
                    except websockets.exceptions.ConnectionClosed:
                        get_data(host)
                    print('done'+host)
            else:
                if(host.find('bibox')>0):
                    message = base64.b64decode(json.loads(message.replace('[','').replace(']',''))['data'])
                    message = gzip.decompress(message)
                    message = message.decode()
                    # print(message)
                message = json.loads(message)
                msg = update_json(message, host)
                if(host.find('binance')>0):
                    msg['bids'] = message['bids']
                    msg['ask'] = message['asks']
                if(host.find('biki')>0):
                    msg['bids'] = message['depth']['bids']
                    msg['ask'] = message['depth']['asks']
                if(host.find('bibox')>0):
                    len_bid = len(message['bids'])
                    msg['bids'] = message['bids'][len_bid-11:len_bid]
                    msg['ask'] = message['asks'][len_bid-11:len_bid]
                await send_to_server(msg, host)


async def handler(connections):
    await asyncio.wait([get_data(uri) for uri in connections])

def main():
    connections = set()
    connections.add('wss://stream.binance.com:9443/ws/btcusdt@depth5@100ms')
    connections.add('wss://push.bibox.com/')
    connections.add('wss://www.lbkex.net/ws/V2/')
    # connections.add('ws://localhost:8765/')
    asyncio.get_event_loop().run_until_complete(handler(connections))

if __name__=="__main__":
    main()
