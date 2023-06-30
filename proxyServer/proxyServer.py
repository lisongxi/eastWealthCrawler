"""模拟一个代理服务器
本地运行
学习一下
"""

# import json
# import socket
#
# import uvicorn
# from fastapi import FastAPI
# import requests
#
# app = FastAPI()
#
#
# @app.get("/")
# async def root():
#     return {"message": "Hello World"}
#
#
# @app.get("/myproxy")
# async def proxy(oriUrl: str):
#     print("i am proxy")
#     response = requests.get(oriUrl)
#     return response.content
#
#
# @app.get("/info/{authkey}")
# async def get_proxy(authkey):
#     # 模拟代理服务器返回的信息。这里都是返回本机的
#     proxy_ip = socket.gethostbyname(socket.gethostname())
#     server = '127.0.0.1:8008'
#     area = 'China'
#     isp = 'China Telecom'
#     deadline = '2050-12-12 12:30:45'
#
#     # 将信息打包成一个JSON对象并返回
#     if authkey == "helloworld":
#         return json.dumps({
#             'data': [{
#                 'proxy_ip': proxy_ip,
#                 'server': server,
#                 'area': area,
#                 'isp': isp,
#                 'deadline': deadline
#             }],
#             'code': 'SUCCESS'
#         })
#     else:
#         return json.dumps({'code': 'authKeyError'})
#
#
# @app.get("/proxy")
# def proxy(url: str, params: dict = None, proxies: dict = None):
#     print("666")
#     response = requests.get(url, params=params, proxies=proxies)
#     return response.text
#
#
# if __name__ == "__main__":
#     uvicorn.run('proxyServer:app', host='127.0.0.1', port=8008, reload=True)
