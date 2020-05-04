import requests

url = 'https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m'

import pymysql
db = pymysql.connect("database-1.cdlbcg6xi4ia.us-east-2.rds.amazonaws.com", "admin", "fJJ45PP4q6t6f5xv", "candles", 3306)
cursor = db.cursor()
cursor.execute("SELECT * FROM CANDLE_TEST LIMIT 10")
print(cursor.fetchall())

r = requests.get(url = url) 
  
# extracting data in json format 
data = r.json() 

for candle in data:
    t = candle[0]
    o = candle[1]
    h = candle[2]
    l = candle[3]
    c = candle[4]
    sql = "INSERT INTO CANDLE_FINAL (TIME, OPEN, HIGH, LOW, CLOSE) VALUES ('"+str(t)+"', "+o+", "+h+", "+l+", "+c+")"
    print(sql)
    cursor.execute(sql)
    db.commit()
print(data)