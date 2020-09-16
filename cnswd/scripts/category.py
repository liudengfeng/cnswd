import click
import pandas as pd

from cnswd.ths.hq import THSDY, THSGN, THSHY, ZJHHY

from ..mongodb import get_db
from ..utils import is_trading_time

NAME_MAPS = {
    '证监会行业分类': ZJHHY,
    '同花顺行业分类': THSHY,
    '地域分类': THSDY,
    '股票概念分类': THSGN
}


def create_index_for(collection):
    # 不存在索性信息时，创建索引
    if not collection.index_information():
        collection.create_index([("编码", 1)], unique=True)


def need_refresh(collection, info):
    """是否需要刷新

    简单规则：
        如果已经存在数据，且24小时内已经刷新 ❌ 否则 ✔
    """
    now = pd.Timestamp('now')
    doc = collection.find_one({'编码': info.编码})
    if doc and now - doc['更新时间'] < pd.Timedelta(days=1):
        return False
    return True


def insert_or_update(collection, doc):
    """插入或更新集合中的文档"""
    old = collection.find_one({'编码': doc['编码']})
    if old:
        # 系统会自动保留原_id
        # doc['_id'] = old['_id']
        collection.find_one_and_replace({'编码': old['编码']}, doc)
    else:
        collection.insert_one(doc)


def refresh():
    """刷新股票概念、行业、地域分类信息"""
    if is_trading_time():
        click.echo('在交易时段内获取股票概念分类数据会导致数据失真')
        return
    db = get_db()
    for name, class_ in NAME_MAPS.items():
        collection = db[name]
        create_index_for(collection)
        with class_() as api:
            head_list = api.get_categories()
            for info in head_list:
                if not need_refresh(collection, info):
                    api.logger.info(f'{info.名称} 24小时内已经刷新')
                    continue
                info = api.get_stock_code_list_of_category(info)
                doc = info.__dict__
                doc['更新时间'] = pd.Timestamp('now')
                insert_or_update(collection, doc)
                api.logger.info(f'更新{name} "{info.名称}"文档')
            # 删除缓存
            api.driver.delete_all_cookies()