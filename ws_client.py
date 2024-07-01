import asyncio
import websockets
import json

async def send_request():
    uri = "ws://127.0.0.1:3000/batch"
    async with websockets.connect(uri) as websocket:
        request_data = {"param1": "value1", "param2": 123}
        await websocket.send(json.dumps(request_data))

        while True:
            try:
                response = await websocket.recv()
            except Exception as e:
                break
            if response == "end":
                print("Received termination signal, closing connection.")
                break
            print(f"Response: {response}")

asyncio.get_event_loop().run_until_complete(send_request())