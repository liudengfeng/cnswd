import asyncio
# import random
# import time
from datetime import datetime

import pandas as pd
from pymongo.errors import DuplicateKeyError

from ..mongodb import get_db
from ..utils import ensure_dtypes, make_logger
from ..websource.ths_news import fetch_news

collection_name = '同花顺财经'
logger = make_logger(collection_name)


def get_max_id():
    db = get_db()
    collection = db[collection_name]
    pipe = [
        {
            '$sort': {
                'id': -1
            }
        },
        {
            '$project': {
                '_id': 0,
                'id': 1
            }
        },
        {
            '$limit': 1
        },
    ]
    try:
        return list(collection.aggregate(pipe))[0]['id']
    except Exception:
        return 0


def _add(collection, docs, init):
    if not init:
        max_id = get_max_id()
        filtered = list(filter(lambda x: x['id'] > max_id, docs))
        if len(filtered):
            collection.insert_many(filtered)
        logger.info(f"Insert {len(filtered)} docs")
    else:
        count = 0
        for doc in docs:
            try:
                collection.insert_one(doc)
                count += 1
            except DuplicateKeyError:
                print(f"{doc['id']} {doc['title']}")
        logger.info(f"Insert {count} docs")


async def refresh(pages=3, init=False):
    """刷新"""
    db = get_db()
    collection = db[collection_name]
    if init:
        # 初始化只能取半年的数据
        collection.drop()
        create_index(collection)
    # 逆序
    # for p in range(pages, 0, -1):
    #     docs = await fetch_news(p)
    #     _add(collection, docs, init)
    # for p in range(pages, 0, -1):
    docs = await fetch_news(pages)
    _add(collection, docs, init)


def create_index(collection):
    collection.create_index([("id", -1)], unique=True, name='id_index')
    collection.create_index([("ctime", -1)], name='dt_index1')
    collection.create_index([("rtime", -1)], name='dt_index2')


if __name__ == '__main__':
    # 初始化只能取半年的数据
    asyncio.run(refresh(100, True))
