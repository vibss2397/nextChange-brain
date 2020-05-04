import asyncio
import websockets

async def hello():
    uri = "ws://3.16.206.27:8765/order-book?ex=lbk&pair=btc-usdt"
    async with websockets.connect(uri) as websocket:
        print('something working')
        message = await websocket.recv()
        print(message)

asyncio.get_event_loop().run_until_complete(hello()) 