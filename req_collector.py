# request_collector.py
import asyncio
import websockets
import redis
import json
import uuid

r = redis.Redis(host='localhost', port=6379, db=0)
clients = {}

async def collect_request(websocket, path):
    async for message in websocket:
        data = json.loads(message)
        
        if data['action'] == 'collect':
            request_id = str(uuid.uuid4())
            r.rpush('request_queue', json.dumps({'id': request_id, 'data': data['payload']}))
            await websocket.send(json.dumps({'status': 'success', 'request_id': request_id}))
        
        elif data['action'] == 'register':
            request_id = data['request_id']
            clients[request_id] = websocket
            await websocket.send(json.dumps({'status': 'success', 'request_id': request_id}))

async def send_result(request_id, result):
    if request_id in clients:
        await clients[request_id].send(json.dumps({'request_id': request_id, 'result': result}))
        del clients[request_id]

async def main():
    server = await websockets.serve(collect_request, 'localhost', 5000)
    await server.wait_closed()

if __name__ == '__main__':
    asyncio.run(main())