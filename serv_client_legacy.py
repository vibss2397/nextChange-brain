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

async def send_to_server(obj):
    uri = "ws://localhost:8765"
    async with websockets.connect(uri) as websocket:
        await websocket.send(obj)

def initialize_object(values):
    global temp_json
    temp_json['binance'] = {}
    temp_json['binance']['open'] = values[0]
    temp_json['bibox'] = {}
    temp_json['bibox']['open'] = values[1]
    temp_json['lbank'] = {}
    temp_json['lbank']['open'] = values[2]

async def update_json(message, host):
    global temp_json, mutex_lock, t1
    if(mutex_lock==0):
        message = json.loads(message)
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

        ##
        # CHANGE REQUEST
        # Maybe client only sends raw data to the server
        # then server figures out open/close/high/low based on
        # it's own choice of precision and its individual clock.
        # That way we can have varying timeframes for the same data.
        async with websockets.connect('ws://localhost:8765/') as webs:
            new_json = {}
            new_json['host'] = host_temp
            new_json['time'] = time.time()
            new_json['last_traded'] = temp_json[host_temp]['last_traded']
            print('asdasdas')
            await webs.send(json.dumps(new_json))

        # If change applied, this becomes obsolete.
        diff = time.time()-t1
        if(diff>=2):
            mutex_lock = 1
            if('last_traded' in temp_json['binance']):
                temp_json['binance']['close'] = temp_json['binance']['last_traded']
            else:
                temp_json['binance']['close'] = 0
                temp_json['binance']['high'] = 0
                temp_json['binance']['low'] = 0

            if('last_traded' in temp_json['bibox']):
                temp_json['bibox']['close'] = temp_json['bibox']['last_traded']
            else:
                temp_json['bibox']['close'] = 0
                temp_json['bibox']['high'] = 0
                temp_json['bibox']['low'] = 0

            if('last_traded' in temp_json['lbank']):
                temp_json['lbank']['close'] = temp_json['lbank']['last_traded']
            else:
                temp_json['lbank']['close'] = 0
                temp_json['lbank']['high'] = 0
                temp_json['lbank']['low'] = 0

            #code to transmit data to webserver
            # async with websockets.connect('ws://localhost:8765/') as webs:
            #     new_json = {}
            #     new_json['host'] = host_temp
            #     new_json['last_traded'] = temp_json[host_temp]['last_traded']
            #     await webs.send(json.dumps(new_json))
                # mes = await webs.recv()
                # print('recieved a messageeeeeeeeeeeeeeeeeeeeeeeee')
                # print(mes)
            temp_arr = [ temp_json['binance']['close'],  temp_json['bibox']['close'], temp_json['lbank']['close']]
            initialize_object(temp_arr)
            mutex_lock = 0
            t1 = time.time()
        #     return 1
        # return 0
    print(diff)
    #print(temp_json)
    print(new_json)

async def get_data(host, ):
    global mutex_lock, temp_json
    global bibox_json,lbank_json
    initialize_object([0, 0, 0])
    async with websockets.connect(host, ssl=ssl.SSLContext()) as websocket:
        if(host.find('bibox')>0):
            await websocket.send(json.dumps(bibox_json))
        elif(host.find('lbk')>0):
            await websocket.send(json.dumps(lbank_json))
        while True:
            message = await websocket.recv()
            if('ping' in message):
                if(host.find('bibox')>0):
                    print('ping'+str(json.loads(message)))
                    await websocket.send(json.dumps({'pong':json.loads(message)['ping']}))
                elif(host.find('lbk')>0):
                    print('ping'+str(json.loads(message)))
                    await websocket.send(json.dumps({'action':'pong', 'pong':json.loads(message)['ping']}))
            else:
                if(host.find('bibox')>0):
                    message = base64.b64decode(json.loads(message.replace('[','').replace(']',''))['data'])
                    message = gzip.decompress(message)
                    message = message.decode()
                    # print(message)
                await update_json(message, host)
                # if(res==1):
                #     await send_to_server(temp_json)


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
