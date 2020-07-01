# from urllib import parse
# import requests

# from cnswd.cninfo.pw import PW


# def gettoken(client_id, client_secret):
#     # api.before.com需要根据具体访问域名修改
#     # netloc = parse.urlparse(target_url).netloc
#     url = 'http://webapi.cninfo.com.cn/api-cloud-platform/oauth2/token'
#     post_data = {"grant_type": "client_credentials",
#                  "client_id": client_id,
#                  "client_secret": client_secret}
#     r = requests.post(url, data=post_data)
#     return r.json()['access_token']


# token = gettoken(PW['id'], PW['password'])



# class Cninfo(object):
#     @classmethod
#     def query_data(cls, kw):
#         pass

#     @classmethod
#     def classify(cls, platetype=137008):
#         pass


# data = {
#     'platetype': 137008,
#     'access_token': token,
#     # 'format': 'json'
# }

# url = "http://webapi.cninfo.com.cn/api/stock/p_public0004"
# # r = requests.get(url, params=params)
# r = requests.post(url, data=data)
# r.json()

# r.text
# r.encoding

# r.encoding = 'utf8'

