# batch_scheduler.py
import redis
import json
import time
import requests

r = redis.Redis(host='localhost', port=6379, db=0)
BATCH_SIZE = 5  # 每次批处理的请求数量
BATCH_INTERVAL = 10  # 批处理的时间间隔（秒）

def batch_requests():
    while True:
        batch = []
        for _ in range(BATCH_SIZE):
            request_data = r.lpop('request_queue')
            if request_data:
                batch.append(json.loads(request_data))
        
        if batch:
            response = requests.post('http://localhost:6000/process_batch', json=batch)
            print(f'Batch processed with response: {response.json()}')
        
        time.sleep(BATCH_INTERVAL)

if __name__ == '__main__':
    batch_requests()