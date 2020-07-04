import time

import pandas as pd

from ..cninfo.classify_tree import ClassifyTree
from ..mongodb import get_db

db = get_db()


def refresh():
    # 覆盖式更新
    s = time.time()
    with ClassifyTree() as api:
        collection_name = '股票分类'
        collection = db[collection_name]
        collection.drop()
        for doc in api.yield_classify_tree():
            collection.insert_one(doc)
        collection_name = '分类BOM'
        collection = db[collection_name]
        collection.drop()
        for docs in api.yield_classify_tree_bom():
            if len(docs):
                collection.insert_many(docs)

    duration = time.time() - s
    api.logger.info(f"耗时{duration:.4f}秒")
