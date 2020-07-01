from pymongo import MongoClient


class Connect(object):
    @staticmethod
    def get_connection():
        # 使用默认值
        return MongoClient()
