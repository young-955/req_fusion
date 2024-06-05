from flask import Flask, request, jsonify
import threading
import time
import queue
import uuid
from config import batch_num, batch_timeout

app = Flask(__name__)

request_queue = []
response_queues = {}
lock = threading.Lock()

# 批量请求处理，受延迟容忍时间控制
def process_batch_requests():
    global request_queue
    while True:
        # 对批量请求的延迟容忍
        time.sleep(batch_timeout)
        batch_requests = None
        with lock:
            if request_queue:
                # 取前n个
                batch_requests = request_queue[:batch_num]
                request_queue = request_queue[batch_num:]
        
        if batch_requests:
            print(f'time process: {batch_requests}')
            batch_request_bodies = [req['body'] for req in batch_requests]

            # 模拟处理批处理请求
            batch_response_bodies = [{**body, 'result': f'Processed {index}'} for index, body in enumerate(batch_request_bodies)]

            # 拆分结果并返回
            for request, response_body in zip(batch_requests, batch_response_bodies):
                request_id = request['id']
                response_queue = response_queues.pop(request_id, None)
                if response_queue:
                    response_queue.put(response_body)

# 处理一次批量请求
def process_batch_requests_once():
    global request_queue
    batch_requests = None
    with lock:
        if request_queue:
            # 取前n个
            batch_requests = request_queue[:batch_num]
            request_queue = request_queue[batch_num:]
    
    if batch_requests:
        print(f'once process: {batch_requests}')
        batch_request_bodies = [req['body'] for req in batch_requests]

        # 模拟处理批处理请求
        batch_response_bodies = [{**body, 'result': f'Processed {index}'} for index, body in enumerate(batch_request_bodies)]

        # 拆分结果并返回
        for request, response_body in zip(batch_requests, batch_response_bodies):
            request_id = request['id']
            response_queue = response_queues.pop(request_id, None)
            if response_queue:
                response_queue.put(response_body)

@app.route('/batch', methods=['POST'])
def batch():
    request_id = str(uuid.uuid4())
    request_data = request.get_json()
    response_queue = queue.Queue()

    with lock:
        request_queue.append({'id': request_id, 'body': request_data})
        response_queues[request_id] = response_queue

    # 如果达到需要批处理的数量，立即执行一次批处理
    print(f'len: {len(request_queue)}')
    if len(request_queue) >= batch_num:
        print(request_queue)
        process_batch_requests_once()

    # 等待结果返回
    result = response_queue.get(timeout=10)  # 设置超时时间为10秒
    return jsonify(result)

if __name__ == '__main__':
    threading.Thread(target=process_batch_requests, daemon=True).start()
    app.run(port=3000)