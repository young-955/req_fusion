from flask import Flask, request, jsonify, Response
from fastapi import WebSocket, WebSocketDisconnect
from batch_infer import batch_fuse_server
import threading
from service_config import connType, connectType
from loguru import logger
import json
from service_config import service_port

app = Flask(__name__)
batch_server = batch_fuse_server()


"""
    推理服务
"""
# 根据配置选择连接类型
if connectType[connType] == connectType.http:
    @app.route('/api/v1/tags/execute', methods=['POST'])
    def batch():
        reqbody = request.get_json()
        query_params = request.args.to_dict()
        res = batch_server.infer(reqbody, query_params)
        # 指定返回类型为application/json
        res = Response(response=json.dumps(res, ensure_ascii=False), mimetype='application/json')
        return res
elif connectType[connType] == connectType.ws:
    @app.websocket("/api/predict")
    async def websocket_endpoint(websocket: WebSocket):
        await websocket.accept()
        try:
            while True:
                request_data = await websocket.receive_json()
                async for response in batch_server.ws_infer(request_data, websocket):
                    if response.get('res', '') == 'end':
                        raise WebSocketDisconnect
                    else:
                        print(f'now response: {response}')
                    await websocket.send_json(response)
        except WebSocketDisconnect:
            print("Client disconnected")
elif connectType[connType] == connectType.redis:
    @app.route('/api/predict', methods=['POST'])
    def batch():
        reqbody = request.get_json()
        logger.info(f'infer start ...')
        text = reqbody["text"]
        history = reqbody.get('history', [])

        res = batch_server.infer({'text': text, 'history': history})
        res = Response(json.dumps(res, ensure_ascii=False))
        return res

if __name__ == '__main__':
    # 单独开个进程走定时任务
    threading.Thread(target=batch_server.process_batch_requests, daemon=True).start()
    app.run(host='0.0.0.0', port=service_port)
