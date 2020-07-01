import pymongo
from retry.api import retry_call

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


def fecth_news(pages):
    with Sina247News() as api:
        df = api.history_news(pages)
    return df


def refresh(pages):
    db = get_db()
    collection = db[collection_name]
    id_ = get_max_id(collection)
    if not id_:
        create_index(collection)
        pages = 10000
    df = retry_call(fecth_news, [pages], tries=3)
    df = ensure_dtypes(df, **col_dtypes)
    df = df.loc[df['序号'] > id_, :]
    data = to_dict(df)
    collection.insert_many(data)
    logger.info(f"插入 {df.shape[0]} 行")


def create_index(collection):
    collection.create_index([("序号", -1)], unique=True, name='id_index')
    collection.create_index([("时间", -1)], name='dt_index')
    # 以下失败，改用 spacy 规则匹配实现搜索及标识
    # collaction = pymongo.collation.Collation('zh')
    # collection.create_index([("概要", "text")],
    #                         name='text',
    #                         collation=collaction)
