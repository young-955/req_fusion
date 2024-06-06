from flask import Flask, request, jsonify
from batch_infer import batch_fuse_server
import threading

app = Flask(__name__)
batch_server = batch_fuse_server()

@app.route('/batch', methods=['POST'])
def batch():
    request_data = request.get_json()
    res = batch_server.infer(request_data)
    return jsonify(res)


if __name__ == '__main__':
    threading.Thread(target=batch_server.process_batch_requests, daemon=True).start()
    app.run(port=3000)