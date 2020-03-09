import asyncio
import websockets
import ssl
import json



async def get_data(host, ):
    ws = host
    async with websockets.connect(ws, ssl=ssl.SSLContext(protocol=ssl.PROTOCOL_TLS)) as websocket:
        while True:
            if(host.find('depth')>0):
                print('raw')
            elif(host.find('agg')>0):
                print('stream')
            message = await websocket.recv()
            print(message)

async def handler(connections):
    await asyncio.wait([get_data(uri) for uri in connections])

def main():
    connections = set()
    connections.add('wss://stream.binance.com:9443/ws/btcusdt@depth5@100ms')
    connections.add('wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade')
    asyncio.get_event_loop().run_until_complete(handler(connections))

if __name__=="__main__":
    main()