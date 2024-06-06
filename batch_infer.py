from service_configs.default_config import inferNeedConn, connType, infer_url, infer_header
import requests
import threading
import time
import queue
import uuid
from service_configs.default_config import batch_num, batch_merge_timeout, batch_process_timeout

class batch_fuse_server():
    def __init__(self) -> None:
        self.request_queue = []
        self.response_queues = {}
        self.lock = threading.Lock()

    # 批量请求处理，受延迟容忍时间控制
    def process_batch_requests(self):
        while True:
            # 对批量请求的延迟容忍
            time.sleep(batch_merge_timeout)
            batch_requests = None
            with self.lock:
                if self.request_queue:
                    # 取前n个
                    batch_requests = self.request_queue[:batch_num]
                    self.request_queue = self.request_queue[batch_num:]
            
            if batch_requests:
                print(f'time process: {batch_requests}')

                # 模拟处理批处理请求
                batch_input = [req['body'] for req in batch_requests]
                batch_response_bodies = batch_inference(batch_input)

                # 拆分结果并返回
                for request, response_body in zip(batch_requests, batch_response_bodies):
                    request_id = request['id']
                    response_queue = self.response_queues.pop(request_id, None)
                    if response_queue:
                        response_queue.put(response_body)

    # 处理一次批量请求
    def process_batch_requests_once(self):
        batch_requests = None
        with self.lock:
            if self.request_queue:
                # 取前n个
                batch_requests = self.request_queue[:batch_num]
                self.request_queue = self.request_queue[batch_num:]
        
        if batch_requests:
            print(f'once process: {batch_requests}')
            batch_request_bodies = [{'id': req['id'], 'body': req['body']} for req in batch_requests]

            # 模拟处理批处理请求
            batch_response_bodies = [{**body, 'result': f'Processed {index}'} for index, body in enumerate(batch_request_bodies)]

            # 拆分结果并返回
            for request, response_body in zip(batch_requests, batch_response_bodies):
                request_id = request['id']
                response_queue = self.response_queues.pop(request_id, None)
                if response_queue:
                    response_queue.put(response_body)

    def infer(self, request_data):
        request_id = str(uuid.uuid4())
        response_queue = queue.Queue()

        with self.lock:
            self.request_queue.append({'id': request_id, 'body': request_data})
            self.response_queues[request_id] = response_queue

        # 如果达到需要批处理的数量，立即执行一次批处理
        print(f'len: {len(self.request_queue)}')
        if len(self.request_queue) >= batch_num:
            print(self.request_queue)
            self.process_batch_requests_once()

        # 等待结果返回
        result = response_queue.get(timeout=batch_process_timeout)  # 设置超时时间为10秒
        print(f'result: {result}')
        return result


def batch_inference(batch_input):
    if inferNeedConn:
        if connType == "http":
            batch_res = requests.post(infer_url, headers=infer_header, json=batch_input)
        elif connType == "ws":
            pass
    else:
        batch_res = batch_input

    return batch_res