from typing import Optional
from fastapi import Request, FastAPI
from fastapi import APIRouter
from fastapi.encoders import jsonable_encoder
from starlette.responses import JSONResponse
import uvicorn

app = FastAPI()
router = APIRouter()

@router.post('/am_fetchdata')
async def smart_fetch_service(request: Request):
    query_info = await request.body()
    print(query_info)

    res = {'status': 0, 'msg': 'qa成功'}
    json_compatible_item_data = jsonable_encoder(res)
    return JSONResponse(content=json_compatible_item_data)

app.include_router(router, prefix='/api', tags=['/api'])

if __name__ == '__main__':
    uvicorn.run('test:app', host='127.0.0.1', port=8080)