"""
主要指数代码
"""
import pandas as pd

from ..mongodb import get_db
from ..utils import make_logger
from ..websource.wy import get_index_base

logger = make_logger('指数代码列表')


def get_index_codes():
    """主要指数代码

    Returns:
        list: 指数代码列表
    """
    df = get_index_base()
    return df.index.tolist()


def _refresh(collection):
    collection.drop()
    doc = {'codes': get_index_codes(), 'update_time': pd.Timestamp.now()}
    collection.insert_one(doc)


def refresh():
    db = get_db('stockdb')
    collection = db['指数列表']
    collection.drop()
    _refresh(collection)
