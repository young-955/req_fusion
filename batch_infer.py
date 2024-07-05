import requests
import threading
import time
import queue
import uuid
from service_configs import connectType, inferNeedConn, connType, infer_url, infer_header, batch_num, batch_merge_timeout, batch_process_timeout
from websocket import create_connection
import json
import asyncio

class batch_fuse_server():
    def __init__(self) -> None:
        self.request_queue = []
        self.response_queues = {}
        self.lock = threading.Lock()
        self.batch_process_flag = False

    # 批量请求处理，受延迟容忍时间控制
    def process_batch_requests(self):
        while True:
            # 对批量请求的延迟容忍
            time.sleep(batch_merge_timeout)
            print(f'time process')
            self.process_batch_requests_once()

    # 处理一次批量请求，直到队列中请求全部处理完再结束
    def process_batch_requests_once(self):
        # 如果已经在处理，则等待上一batch处理完再执行
        if self.batch_process_flag:
            return
        else:
            self.batch_process_flag = True
        
        while len(self.request_queue) > 0:
            batch_requests = None
            with self.lock:
                if self.request_queue:
                    # 取前n个
                    batch_requests = self.request_queue[:batch_num]
                    self.request_queue = self.request_queue[batch_num:]
            
            if batch_requests:
                print(f'once process: {batch_requests}')

                # 模拟处理批处理请求
                batch_input = [req['body'] for req in batch_requests]
                # batch_response_bodies = batch_inference(batch_input)

                if connectType[connType] == connectType.ws:
                    # 流式返回
                    # TODO 增加batch中多个请求结束不一致的处理
                    for batch_response_bodies in batch_inference(batch_input):
                        batch_id = [req['id'] for req in batch_requests]
                        # 拆分结果并返回
                        for request, response_body in zip(batch_requests, batch_response_bodies):
                            request_id = request['id']
                            # 流式已完成推理的id不进行处理
                            if request_id in batch_id:
                                response_queue = self.response_queues.get(request_id)
                                if response_queue:
                                    if response_body.get('res', '') == 'end':
                                        batch_id.remove(request_id)
                                    response_queue.put(response_body)
                else:
                    batch_response_bodies = list(batch_inference(batch_input))
                    # 拆分结果并返回
                    for request, response_body in zip(batch_requests, batch_response_bodies):
                        request_id = request['id']
                        response_queue = self.response_queues.pop(request_id, None)
                        if response_queue:
                            response_queue.put(response_body)
            
        self.batch_process_flag = False
        print('once process finished')

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

    async def ws_infer(self, request_data, websocket):
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
        while True:
            try:
                result = await asyncio.get_event_loop().run_in_executor(None, response_queue.get, batch_process_timeout)
                print(f'result: {result}')
                yield result
                if result.get('res', '') == 'end':
                    raise Exception('end request')
            except Exception as e:
                # 销毁本queue
                self.response_queues.pop(request_id, None)
                return

def batch_inference(batch_input):
    import time
    time.sleep(10)
    if inferNeedConn:
        # 推理服务与本服务需要通过请求连接
        if connectType[connType] == connectType.http:
            batch_res = requests.post(infer_url, headers=infer_header, json=batch_input)
        elif connectType[connType] == connectType.ws:
            ws = create_connection(infer_url)
            try:
                qs = json.dumps(batch_input, ensure_ascii=False)

                ws.send(qs)
                while True:
                    data = json.loads(ws.recv())
                    yield data
            # 异常前先把连接关了
            except Exception as e:
                ws.close()
                raise Exception
    else:
        if connectType[connType] == connectType.http:
            return batch_input
        elif connectType[connType] == connectType.ws:
            # 推理服务在本服务中实现
            batch_res = batch_input
            yield batch_res
            yield [{'res': 'add1'}] * len(batch_res)
            yield [{'res': 'add2'}] * len(batch_res)
            yield [{'res': 'end'}] * len(batch_res)
            return

    return batch_res