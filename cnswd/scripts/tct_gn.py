"""
腾讯概念股票列表(覆盖式更新)
"""
import pandas as pd

from ..mongodb import get_db
from ..utils import make_logger
from ..websource.tencent import fetch_concept_stocks

collection_name = '腾讯概念'
logger = make_logger(collection_name)


def create_index(collection):
    collection.create_index([("股票代码", 1)], name='code_index')
    collection.create_index([("概念id", 1)], name='id_index')


def refresh():
    """采用覆盖式更新腾讯股票概念列表"""
    df = fetch_concept_stocks()
    grouped = df.groupby(['item_id', 'item_name'])
    docs = []
    for name, group in grouped:
        d = {
            '概念编码': name[0],
            "概念名称": name[1],
            "股票列表": group['code'].values.tolist(),
            "更新时间": pd.Timestamp('now'),
        }
        docs.append(d)
    db = get_db()
    collection = db[collection_name]
    collection.drop()
    create_index(collection)
    collection.insert_many(docs)
    logger.info(f"行数 {len(docs)}")
