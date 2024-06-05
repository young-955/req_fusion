# response_distributor.py
import redis
import json
import time

r = redis.Redis(host='localhost', port=6379, db=0)

def distribute_responses():
    while True:
        result_data = r.lpop('result_queue')
        if result_data:
            result = json.loads(result_data)
            request_id = result['id']
            # 将结果存储在Redis中，以便请求者能够查询
            r.set(f'result_{request_id}', json.dumps(result))
            print(f'Response for request {request_id}: {result["result"]}')
        
        time.sleep(1)

if __name__ == '__main__':
    distribute_responses()