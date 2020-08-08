import pymongo
from pymongo.errors import DuplicateKeyError
from retry.api import retry_call

from ..mongodb import get_db
from ..websource.sina_news import Sina247News, logger

collection_name = "新浪财经"


def get_max_id(collection):
    pipe = [
        {
            '$sort': {
                '序号': -1
            }
        },
        {
            '$project': {
                '_id': 0,
                '序号': 1
            }
        },
        {
            '$limit': 1
        },
    ]
    try:
        return list(collection.aggregate(pipe))[0]['序号']
    except (IndexError, ):
        return 0


def refresh(pages):
    db = get_db()
    collection = db[collection_name]
    id_ = get_max_id(collection)
    if id_ == 0:
        create_index(collection)
        pages = 2000
        logger.info(f"初始设置页数：{pages}")
    with Sina247News() as api:
        for docs in api.yield_history_news(pages):
            count = 0
            for doc in docs:
                try:
                    collection.insert_one(doc)
                    count += 1
                except DuplicateKeyError:
                    pass
            logger.info(f"新增 {count} 行")


def create_index(collection):
    collection.create_index([("序号", -1)], unique=True, name='id_index')
    # collection.create_index([("序号", -1)], name='id_index')
    collection.create_index([("时间", -1)], name='dt_index')
