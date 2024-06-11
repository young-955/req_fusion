import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from starlette.responses import JSONResponse
from batch_infer import batch_fuse_server
import threading
from service_configs import connType, connectType

app = FastAPI()
batch_server = batch_fuse_server()

# 根据配置选择连接类型
if connectType[connType] == connectType.http:
    @app.post('/batch', methods=['POST'])
    def batch(request: Request):
        request_data = request.get_json()
        res = batch_server.infer(request_data)
        return JSONResponse(res)
elif connectType[connType] == connectType.ws:
    @app.websocket("/batch")
    async def websocket_endpoint(websocket: WebSocket):
        await websocket.accept()
        try:
            while True:
                request_data = await websocket.receive_json()
                async for response in  batch_server.ws_infer(request_data, websocket):
                    if response.get('res', '') == 'end':
                        raise WebSocketDisconnect
                    else:
                        print(f'now response: {response}')
                    await websocket.send_json(response)
        except WebSocketDisconnect:
            print("Client disconnected")
elif connectType[connType] == connectType.redis:
    # 大模型流式
    @app.websocket("/batch")
    async def websocket_endpoint(websocket: WebSocket):
        await websocket.accept()
        try:
            while True:
                request_data = await websocket.receive_json()
                async for response in  batch_server.ws_infer(request_data, websocket):
                    if response.get('res', '') == 'end':
                        raise WebSocketDisconnect
                    else:
                        print(f'now response: {response}')
                    await websocket.send_json(response)
        except WebSocketDisconnect:
            print("Client disconnected")

if __name__ == '__main__':
    threading.Thread(target=batch_server.process_batch_requests, daemon=True).start()
    uvicorn.run(app, host="127.0.0.1", port=3000)