from pymongo import MongoClient
from .setting.constants import DB_HOST


class Connect(object):
    @staticmethod
    def get_connection():
        # 使用默认值
        return MongoClient(DB_HOST, 27017)
