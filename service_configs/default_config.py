from service_configs.util import connectType

# 服务名称，与其他服务区分开，否则会被覆盖
service_name = ''
# 一次最大处理的请求数
batch_num = 2
# 批量请求合并延迟容忍时间/s
batch_merge_timeout = 5
# 等待批处理完成时间
batch_process_timeout = 10
# 推理是否需要通过请求完成
inferNeedConn = False
# 请求连接类型
# 1: http
# 2: ws
# 3: redis
connType = 'http'
# 请求地址
infer_url = 'http://0.0.0.0:8080/api/v1/predict'
# 请求header
infer_header = {"Content-Type": "application/json"}
