import asyncio
import websockets
import json

# array of open/close/high/low for each host.
# concatenated for each minute
candles = {}

import pymysql
db = pymysql.connect("database-1.cdlbcg6xi4ia.us-east-2.rds.amazonaws.com", "admin", "fJJ45PP4q6t6f5xv", "candles", 3306)
cursor = db.cursor()
cursor.execute("SELECT * FROM CANDLE_TEST")
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
    candles[host]["time"] = dt.datetime.now()

# resets candle at the beginning of every minute
def reset_candle(host):
    global candles
    candles[host]["open"] = candles[host]["close"]
    candles[host]["high"] = candles[host]["close"]
    candles[host]["low"] = candles[host]["close"]
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

# print for candle JSON
def print_candle(candle):
    print("Open:", candle["open"], "| Close:", candle["close"], "| High:", candle["high"], "| Low:", candle["low"], "| Time:", candle["time"])

# insert candle into database at the end of each minute
def insert_candle():
    global db, cursor
    global sched
    global candles

    print(candles["binance"])
    t = previous_minute(candles["binance"]["time"]).isoformat()
    o = str(candles["binance"]["open"])
    h = str(candles["binance"]["high"])
    l = str(candles["binance"]["low"])
    c = str(candles["binance"]["close"])
    reset_candle("binance")
    reset_candle("bibox")
    reset_candle("lbank")
    sql = "INSERT INTO CANDLE_TEST (CANDLE_DATE, OPEN, HIGH, LOW, CLOSE) VALUES ('"+t+"', "+o+", "+h+", "+l+", "+c+")"
    cursor.execute(sql)
    db.commit()
    print("Success")
    sched.add_job(insert_candle, 'date', run_date=increment_minute(previous_minute(dt.datetime.now())))

# current hosts
init_candle('binance')
init_candle('bibox')
init_candle('lbank')


# schedule insert_candle job for the scheduler
sched.add_job(insert_candle, 'date', run_date=increment_minute(previous_minute(dt.datetime.now())))
sched.start()

async def spit_out_data(websocket, path):
    global candles
    async for message in websocket:
        print(message)
        message_json = json.loads(message)
        update_candle(message_json["host"], message_json["last_traded"])
        #print_candle(candles[message_json["host"]])
        await websocket.send(message)

start_server = websockets.serve(spit_out_data, "localhost", 8765)

asyncio.get_event_loop().run_until_complete(start_server)
asyncio.get_event_loop().run_forever()
