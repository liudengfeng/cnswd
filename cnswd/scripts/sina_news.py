import pymongo
from retry.api import retry_call
from pymongo.errors import DuplicateKeyError
from ..mongodb import get_db
from ..utils import ensure_dtypes
from ..utils.db_utils import to_dict
from ..websource.sina_news import Sina247News, logger

collection_name = "新浪财经"
col_dtypes = {
    'd_cols': ['时间'],
    's_cols': ['概要', '分类'],
    'i_cols': ['序号'],
}


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
    if not id_:
        create_index(collection)
        pages = 10000
        logger.info(f"初始设置页数：{pages}")
    count = 0
    with Sina247News() as api:
        for df in api.yield_history_news(pages):
            df = ensure_dtypes(df, **col_dtypes)
            docs = to_dict(df)
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
