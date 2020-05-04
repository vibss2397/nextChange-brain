import asyncio
import websockets
import json
import string 
import random
# array of open/close/high/low for each host.
# concatenated for each minute
candles = {}
counter = 0
prev_message = {}
sent_status = 0

USERS = set()
websocket_prop = {}
websocket_map = {}

curr_message = ''
import pymysql
db = pymysql.connect("database-1.cdlbcg6xi4ia.us-east-2.rds.amazonaws.com", "admin", "fJJ45PP4q6t6f5xv", "candles", 3306)
cursor = db.cursor()
cursor.execute("SELECT * FROM CANDLE_TRAIN LIMIT 10")
print(cursor.fetchall())

# APScheduler. BackgroundScheduler can run whatever shit you want
# silently in the background, and doesn't block.
# And APS allows scheduling events at custom date/time
from apscheduler.schedulers.background import BackgroundScheduler

# init job scheduler
sched = BackgroundScheduler()

# datetime is a trigger date format for the scheduler
import datetime as dt

print(type(dt.datetime.now().isoformat()))

#giving an id to all websocket connections
def randomString(stringLength=8):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))

#all the websocket functions
def users_event():
    return json.dumps({"type": "users", "count": len(USERS)})

def get_message(req, exchange, message, pair, frame = 1):
    global candles, prev_message
    if(req=='trade'):
        res = {}
        res['last_traded'] = candles[exchange]['lastTraded']
        res['pair'] = pair
        res['host'] = exchange
        res['time'] =str(dt.datetime.now())
        return json.dumps(res)
    elif(req=='candle'):
        counter = message
        res = {}
        res['CANDLE_DATE'] = str(candles[exchange]['time'])
        res['FRAME'] = frame
        res['CLOSE'] = candles[exchange]['close']
        res['HIGH'] = candles[exchange]['high']
        res['LOW'] = candles[exchange]['low']
        res['OPEN'] = candles[exchange]['open']
        res['pair'] = pair
        res['host'] = exchange
        return json.dumps(res)
    elif(req=='order-book'):
        return prev_message[exchange]

def set_websocket_prop(id, websocket):
    global websocket_prop
    base_find = str(websocket.path).find('?')
    if(base_find>0):
        websocket_prop[id]['type'] = str(websocket.path).split('?')[0][1:]
        if(websocket_prop[id]['type']=='trade'):
            websocket_prop[id]['exchange'] = websocket.path.split('?')[1].split('&')[0].split('=')[1]
            websocket_prop[id]['pair'] = websocket.path.split('?')[1].split('&')[1].split('=')[1]
        elif(websocket_prop[id]['type']=='candle'):
            websocket_prop[id]['exchange'] = websocket.path.split('?')[1].split('&')[0].split('=')[1]
            websocket_prop[id]['frame'] = websocket.path.split('?')[1].split('&')[1].split('=')[1]
            websocket_prop[id]['pair'] = websocket.path.split('?')[1].split('&')[2].split('=')[1]
        elif(websocket_prop[id]['type']=='order-book'):
            websocket_prop[id]['exchange'] = websocket.path.split('?')[1].split('&')[0].split('=')[1]
            websocket_prop[id]['pair'] = websocket.path.split('?')[1].split('&')[1].split('=')[1]
    else:
        websocket_prop[id]['type']= 'base'

async def notify_users(message):
    global counter, sent_status
    if USERS:  # asyncio.wait doesn't accept an empty list
        try:
            for user in USERS:
                message_json = json.loads(message)
                if('count' in message_json):
                    websocket = websocket_prop[user]['websocket']
                    #await websocket.send(message)
                else:
                    websocket = websocket_prop[user]['websocket']
                    req = websocket_prop[user]['type']
                    if(req!='base'):
                        if(message_json['update_type']=='realtime'):
                            if(req=='trade' or req=='order-book'):
                                exchange = websocket_prop[user]['exchange']
                                msg = get_message(req, exchange, message, websocket_prop[user]['pair'])
                                await websocket.send(msg)
                        if(counter>0 and sent_status==1):
                            if(req=='candle'):
                                frame = websocket_prop[user]['frame'][0]
                                frame = int(frame)
                                if(counter%frame==0):
                                    exchange = websocket_prop[user]['exchange']
                                    msg = get_message(req, exchange, counter, websocket_prop[user]['pair'], websocket_prop[user]['frame'])
                                    await websocket.send(msg)
                                sent_status = 0

                            
        except:
            e = sys.exc_info()[0]

async def register(websocket):
    global websocket_prop
    id = randomString(10)
    websocket_prop[id] = {}
    websocket_prop[id]['websocket'] = websocket
    set_websocket_prop(id, websocket)
    websocket_map[websocket] = id
    USERS.add(id)
    await notify_users(users_event())

async def unregister(websocket):
    id = websocket_map[websocket]
    USERS.remove(id)
    if(id in websocket_prop):
        del websocket_prop[id]
    await notify_users(users_event())
     
# cuts seconds and miliseconds
def previous_minute(date):
    return dt.datetime(date.year, date.month, date.day, date.hour, date.minute)

# adds one minute to dt
def increment_minute(date):
    return date + dt.timedelta(minutes=1)

# sets an empty candle
def init_candle(host):
    global candles
    candles[host] = {}
    candles[host]["open"] = -1.0
    candles[host]["close"] = -1.0
    candles[host]["high"] = -1.0
    candles[host]["low"] = -1.0
    candles[host]["lastTraded"] = -1.0
    candles[host]["time"] = dt.datetime.now()

# resets candle at the beginning of every minute
def reset_candle(host):
    global candles
    candles[host]["open"] = candles[host]["close"]
    candles[host]["high"] = candles[host]["close"]
    candles[host]["low"] = candles[host]["close"]
    candles[host]["lastTraded"] = candles[host]["close"]
    candles[host]["time"] = dt.datetime.now()

# updates open/close/high/low with new price
def update_candle(host, price):
    global candles
    candles[host]["close"] = price
    if candles[host]["open"] == -1:
        candles[host]["open"] = price

    if candles[host]["high"] < price:
        if candles[host]["low"] == -1.0:
            candles[host]["low"] = price
        candles[host]["high"] = price
    elif candles[host]["low"] > price:
        candles[host]["low"] = price
    candles[host]['lastTraded'] = price

# print for candle JSON
def print_candle(candle, host):
    print("Host", host, "| Open:", candle["open"], "| Close:", candle["close"], "| High:", candle["high"], "| Low:", candle["low"], "| Time:", candle["time"])

# insert candle into database at the end of each minute
def insert_candle():
    global db, cursor, counter, sent_status
    global sched
    global candles
    counter+=1
    sent_status = 1
    for key in candles:
        t = previous_minute(candles[key]["time"]).isoformat()
        o = str(candles[key]["open"])
        h = str(candles[key]["high"])
        l = str(candles[key]["low"])
        c = str(candles[key]["close"])
        ho = key
        sql = "INSERT INTO CANDLE_TRAIN (CANDLE_DATE, OPEN, HIGH, LOW, CLOSE, HOST) VALUES ('"+t+"', "+o+", "+h+", "+l+", "+c+", '"+ ho+ "')"
        cursor.execute(sql)
        db.commit()
        if(counter%5==0):
            sql = "INSERT INTO CANDLE_FIVE (CANDLE_DATE, OPEN, HIGH, LOW, CLOSE, HOST) VALUES ('"+t+"', "+o+", "+h+", "+l+", "+c+", '"+ ho+ "')"
            cursor.execute(sql)
            db.commit()
        if(counter%10==0):
            sql = "INSERT INTO CANDLE_TEN (CANDLE_DATE, OPEN, HIGH, LOW, CLOSE, HOST) VALUES ('"+t+"', "+o+", "+h+", "+l+", "+c+", '"+ ho+ "')"
            cursor.execute(sql)
            db.commit()
        if(counter%20==0):
            sql = "INSERT INTO CANDLE_TWENTY (CANDLE_DATE, OPEN, HIGH, LOW, CLOSE, HOST) VALUES ('"+t+"', "+o+", "+h+", "+l+", "+c+", '"+ ho+ "')"
            cursor.execute(sql)
            db.commit()
        if(counter%60==0):
            sql = "INSERT INTO CANDLE_HOUR (CANDLE_DATE, OPEN, HIGH, LOW, CLOSE, HOST) VALUES ('"+t+"', "+o+", "+h+", "+l+", "+c+", '"+ ho+ "')"
            cursor.execute(sql)
            db.commit()
            counter = 0
        print_candle(candles[key], key)
        reset_candle(key)
    sched.add_job(insert_candle, 'date', run_date=increment_minute(previous_minute(dt.datetime.now())))

# current hosts
init_candle('binance')
init_candle('bibox')
init_candle('lbank')


# schedule insert_candle job for the scheduler
sched.add_job(insert_candle, 'date', run_date=increment_minute(previous_minute(dt.datetime.now())))
sched.start()

async def spit_out_data(websocket, path):
    global candles, prev_message
    await register(websocket)
    try:
        message = await websocket.recv()
        message_json = json.loads(message)
        prev_message[message_json['host']] = message
        update_candle(message_json["host"], message_json["last_traded"])
        # print_candle(candles[message_json["host"]], message_json['host'])
        message_json['update_type']='realtime'
        await notify_users(json.dumps(message_json))
    except Exception:
        a = '2'
    finally:
        await unregister(websocket)

start_server = websockets.serve(spit_out_data, "localhost", 8765)

asyncio.get_event_loop().run_until_complete(start_server)
asyncio.get_event_loop().run_forever()
