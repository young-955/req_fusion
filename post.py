import requests

# qwen
qwen_url = "http://127.0.0.1:3000/batch"

# 请求的 URL
url = qwen_url  # 替换为实际的接口地址
content = '你好'
# 请求的参数
payload = {
    "model": "qwen1p5_72b",
    "max_tokens": 2046
}
# 设置请求头
headers = {
    "Content-Type": "application/json",
    "serviceId":"Cpic.All.Service.Ai.chat.completions"
}

# 发送请求并获取响应
response = requests.post(url, json=payload)
print(response.text)

# print(response['choices'][0]['message']['content'])

