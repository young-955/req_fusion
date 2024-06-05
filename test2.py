from flask import Flask, request, jsonify
import threading
import time
import queue
import uuid

app = Flask(__name__)

request_queue = []
response_queues = {}
lock = threading.Lock()

def process_batch_requests():
    while True:
        time.sleep(5)  # 每5秒检查一次
        with lock:
            if request_queue:
                batch_requests = request_queue[:]
                request_queue.clear()
        
        if batch_requests:
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

    # 如果达到批处理的数量，立即处理
    if len(request_queue) >= 2:
        import time
        print(request_queue)
        time.sleep(3)
        process_batch_requests()

    # 等待结果返回
    result = response_queue.get(timeout=10)  # 设置超时时间为10秒
    return jsonify(result)

if __name__ == '__main__':
    threading.Thread(target=process_batch_requests, daemon=True).start()
    app.run(port=3000)