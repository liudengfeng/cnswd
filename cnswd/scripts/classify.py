import time

import pandas as pd

from ..cninfo.classify_tree import ClassifyTree
from ..mongodb import get_db

db = get_db()
collection_name = '股票分类'
collection = db[collection_name]


def refresh():
    # 覆盖式更新
    collection.drop()
    s = time.time()
    with ClassifyTree() as api:
        for doc in api.yield_classify_tree():
            collection.insert_one(doc)
    duration = time.time() - s
    api.logger.info(f"耗时{duration:.4f}秒")
