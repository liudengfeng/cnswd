import pandas as pd

from ..cninfo import FastSearcher, ThematicStatistics
from ..mongodb import get_db


def _refresh(db, name, api):
    # 数据浏览器元数据
    collection = db[name]
    collection.drop()

    levels = api.levels

    for info in levels:
        level = info.pos
        meta = api.get_level_meta_data(level)
        meta['更新时间'] = pd.Timestamp('now')
        collection.insert_one(meta)
        api.logger.info(f"写入项目 {info.name} 元数据")
    api.driver.quit()


def refresh():
    # 数据浏览器元数据
    db = get_db('config')
    for name, api in zip(['数据浏览器元数据', '专题统计元数据'],
                         [FastSearcher(), ThematicStatistics()]):
        _refresh(db, name, api)
