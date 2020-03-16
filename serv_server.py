import asyncio
import websockets

async def spit_out_data(websocket, path):
    async for message in websocket:
        print(message)
        await websocket.send(message)

start_server = websockets.serve(spit_out_data, "localhost", 8765)

asyncio.get_event_loop().run_until_complete(start_server)
asyncio.get_event_loop().run_forever()