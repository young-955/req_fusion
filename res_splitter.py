# result_splitter.py
import asyncio
import websockets
import redis
import json

r = redis.Redis(host='localhost', port=6379, db=0)

async def process_batch():
    async with websockets.connect('ws://localhost:5000') as websocket:
        while True:
            batch = []
            for _ in range(5):  # 每次批处理的请求数量
                request_data = r.lpop('request_queue')
                if request_data:
                    batch.append(json.loads(request_data))

            if batch:
                results = []
                for item in batch:
                    # 这里可以加入实际的处理逻辑
                    result = {'id': item['id'], 'result': f"Processed {item['data']}"}
                    results.append(result)
                    r.set(f'result_{item["id"]}', json.dumps(result))
                    
                    # 通知请求收集器发送结果
                    await websocket.send(json.dumps({'action': 'send_result', 'request_id': item['id'], 'result': result}))

            await asyncio.sleep(10)  # 批处理的时间间隔（秒）

if __name__ == '__main__':
    asyncio.run(process_batch())