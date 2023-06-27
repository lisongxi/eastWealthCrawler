"""模拟一个代理服务器
本地运行
学习一下
"""

from flask import Flask, request, jsonify
import requests
import socket

app = Flask(__name__)


# 代理服务器
@app.route('/myproxy', methods=['GET', 'POST', 'PUT', 'DELETE'])
def proxy():
    if request.method == 'GET':
        # 处理GET请求
        urls = request.url
        params = request.args.to_dict()

        # 使用requests库来获取目标网页内容
        response = requests.get(url=urls)
        return response.content
    elif request.method == 'POST':
        # 处理POST请求

        url = request.form.get('url')
        # 使用requests库来发送POST请求并获取响应内容
        data = request.get_data()
        response = requests.post(url, data=data)
        return response.content
    else:
        return "RequestError"


# 获取代理信息
@app.route('/get')
def get_proxy():
    # 模拟代理服务器返回的信息。这里都是返回本机的
    proxy_ip = socket.gethostbyname(socket.gethostname())
    server = '127.0.0.1:5000'
    area = 'China'
    isp = 'China Telecom'
    deadline = '2050-12-12 12:30:45'

    # 获取 GET 请求的 query 参数
    authkey = request.args.get('authkey')

    # 将信息打包成一个JSON对象并返回
    if authkey == "helloworld":
        return jsonify({
            'data': [{
                'proxy_ip': proxy_ip,
                'server': server,
                'area': area,
                'isp': isp,
                'deadline': deadline
            }],
            'code': 'SUCCESS'
        })
    else:
        return jsonify({'code': 'authKeyError'})


if __name__ == '__main__':
    app.run(host='10.6.0.60', port=5000)
