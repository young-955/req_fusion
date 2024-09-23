import requests
import threading
import time
import queue
import uuid
from service_config import connectType, inferNeedConn, connType, infer_url, infer_header, batch_num, batch_merge_timeout, batch_process_timeout
from websocket import create_connection
import json
import asyncio
from loguru import logger
import os
from redis import Sentinel
import redis
import traceback

# 定义全局推理配置
try:
    redis_url = eval(os.environ.get('redis_url', 'xxx'))
except Exception as e:
    redis_url = os.environ.get('redis_url', 'xxx')
redis_port = os.environ.get('redis_port', 31001)
redis_pwd = os.environ.get('redis_pwd', '')
# if you need
project_prefix = os.environ.get('project_prefix', 'xxx')
get_input_postfix = os.environ.get('get_input_postfix', 'xxx')

if connectType[connType] == connectType.redis:
    # 初始化redis
    if os.environ.get('redis_type', 'single') == "single":
        redis_server = redis.Redis(host=redis_url, port=redis_port, db=0)
    elif os.environ['redis_type'] == "sentinel":
        sentinel = Sentinel(sentinels=redis_url)
        master = sentinel.discover_master('mymaster')
        redis_server = redis.Redis(host=master[0], port=master[1], password=redis_pwd)

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
            self.process_batch_requests_once()

    # 处理一次批量请求，直到队列中请求全部处理完再结束
    def process_batch_requests_once(self):
        # 如果已经在处理，则等待上一batch处理完再执行
        if self.batch_process_flag:
            logger.info('func is running, return')
            return
        else:
            self.batch_process_flag = True
        
        while len(self.request_queue) > 0:
            logger.info(f'request_queue: {len(self.request_queue)}')
            batch_requests = None
            with self.lock:
                if self.request_queue:
                    # 取前n个
                    batch_requests = self.request_queue[:batch_num]
                    self.request_queue = self.request_queue[batch_num:]
            
            if batch_requests:
                logger.info(f'once process: {batch_requests}')

                if connectType[connType] == connectType.ws:
                    batch_input = [req['body'] for req in batch_requests]
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
                elif connectType[connType] == connectType.redis:
                    # 记录当前处理中的队列长度
                    cur_queue_len = len(batch_requests)
                    # 将请求传入redis队列
                    input_ids = list(batch_inference(batch_requests))[0]

                    # 等待redis队列中请求处理完成
                    while True:
                        # 有新的请求就推到redis队列
                        if len(self.request_queue) > 0:
                            add_batch_requests = None
                            with self.lock:
                                if self.request_queue:
                                    # 取1个新请求
                                    add_batch_requests = self.request_queue[:1]
                                    self.request_queue = self.request_queue[1:]
                                    cur_queue_len += 1
                            
                            if add_batch_requests:
                                input_ids += list(batch_inference(add_batch_requests))[0]

                        # 检查是否有处理完成的请求
                        del_input_ids = []
                        for input_id in input_ids:
                            queue_len = redis_server.llen(project_prefix + input_id)
                            msg = None
                            if queue_len > 0:
                                msg = redis_server.lpop(project_prefix + input_id)

                                if msg == None:
                                    pass
                                else:
                                    # 读结果并推到结果队列
                                    infer_res = json.loads(msg)
                                    response_queue = self.response_queues.pop(input_id, None)
                                    logger.info(f'this id: {input_id}, response_body: {infer_res}')
                                    if response_queue:
                                        response_queue.put(infer_res)
                                        del_input_ids.append(input_id)
                        
                        # 删除已处理的请求
                        for input_id in del_input_ids:
                            input_ids.remove(input_id)

                        # 退出本轮处理
                        if len(self.request_queue) == 0 and len(input_ids) == 0:
                            break

                        # 没有退出则等待一段时间再进行下一轮查询
                        time.sleep(0.1)
                # http                        
                else:
                    logger.info(f'http infering, input: {batch_requests}')
                    batch_input = [req['body'] for req in batch_requests]
                    req_params = batch_requests[0]['query_params']
                    try:
                        batch_response_bodies = list(batch_inference({'body': batch_input, 'query_params': req_params}))[0]
                    except Exception as e:
                        traceback.print_exc()
                        for request in batch_requests:
                            request_id = request['id']
                            response_queue = self.response_queues.pop(request_id, None)
                        continue
                    logger.info(f'http infer finish, res: {batch_response_bodies}')
                    # 拆分结果并返回
                    for request, response_body in zip(batch_requests, batch_response_bodies):
                        request_id = request['id']
                        response_queue = self.response_queues.pop(request_id, None)
                        logger.info(f'this id: {request_id}, response_body: {response_body}')
                        if response_queue:
                            response_queue.put({'result': response_body})
            
        self.batch_process_flag = False

    # request_data: 请求体数据
    # query_params: 请求可能带有查询字符串，同样传给模型服务，当前只支持相同的查询字符串条件
    def infer(self, request_data, query_params=None):
        request_id = str(uuid.uuid4())
        response_queue = queue.Queue()

        with self.lock:
            self.request_queue.append({'id': request_id, 'body': request_data, 'query_params': query_params})
            self.response_queues[request_id] = response_queue

        # 如果达到需要批处理的数量，立即执行一次批处理
        t1 = time.time()
        logger.info(f'request_queue len: {len(self.request_queue)}, {self.request_queue}')
        if len(self.request_queue) >= batch_num:
            logger.info(self.request_queue)
            self.process_batch_requests_once()

        # 等待结果返回
        try:
            result = response_queue.get(timeout=batch_process_timeout) # 设置超时时间
        except Exception as e:
            result = {'id': request_id}
        t2 = time.time()
        logger.info(f'cost: {t2-t1}, result: {result}')
        return result

    async def ws_infer(self, request_data, websocket):
        request_id = str(uuid.uuid4())
        response_queue = queue.Queue()

        with self.lock:
            self.request_queue.append({'id': request_id, 'body': request_data})
            self.response_queues[request_id] = response_queue

        # 如果达到需要批处理的数量，立即执行一次批处理
        logger.info(f'len: {len(self.request_queue)}')
        if len(self.request_queue) >= batch_num:
            logger.info(self.request_queue)
            self.process_batch_requests_once()

        # 等待结果返回
        while True:
            try:
                result = await asyncio.get_event_loop().run_in_executor(None, response_queue.get, batch_process_timeout)
                logger.info(f'result: {result}')
                yield result
                if result.get('res', '') == 'end':
                    raise Exception('end request')
            except Exception as e:
                # 销毁本queue
                self.response_queues.pop(request_id, None)
                return

def batch_inference(batch_input):
    if inferNeedConn:
        # 推理服务与本服务需要通过请求连接
        if connectType[connType] == connectType.http:
            if batch_input['query_params'] == None:
                batch_res = requests.post(infer_url, headers=infer_header, json=batch_input['body']).json()
            else:
                batch_res = requests.post(infer_url, params=batch_input['query_params'], headers=infer_header, json=batch_input['body']).json()
            logger.info(f'Returned http res: {batch_res}')
            yield batch_res
            return
        elif connectType[connType] == connectType.ws:
            ws = create_connection(infer_url)
            try:
                qs = json.dumps(batch_input, ensure_ascii=False)
                ws.send(qs)
                while True:
                    data = json.loads(ws.recv())
                    logger.info(f'Received ws data: {data}')
                    yield data
            # 异常前先把连接关了
            except Exception as e:
                ws.close()
                raise e
    else:
        if connectType[connType] == connectType.http:
            logger.info(f'Returning mock http res: {batch_input}')
            yield batch_input
            return
        elif connectType[connType] == connectType.ws:
            # 推理服务在本服务中实现
            batch_res = batch_input
            yield batch_res
            yield [{'res': 'add1'}] * len(batch_res)
            yield [{'res': 'add2'}] * len(batch_res)
            yield [{'res': 'end'}] * len(batch_res)
            return
        elif connectType[connType] == connectType.redis:
            input_ids = []
            for per_batch_input in batch_input:
                input_ids.append(per_batch_input['id'])
                redis_server.rpush(project_prefix + get_input_postfix, json.dumps({'key': per_batch_input['id'], 'history': per_batch_input['body']['history'], 'text': per_batch_input['body']['text']}, ensure_ascii=False))
            yield input_ids
            return

    return batch_res
