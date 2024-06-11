from enum import Enum

class connectType(Enum):
    # http
    http = 0
    # websocket
    ws = 1
    # redis，专为大模型流式设计
    redis = 2