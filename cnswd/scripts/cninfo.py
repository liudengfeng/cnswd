"""
必须定期清理
高级搜索以当前可用状态的股票为总体

"""
import sys

import pandas as pd
from pymongo.errors import DuplicateKeyError

from .._exceptions import FutureDate
from ..cninfo import AdvanceSearcher, FastSearcher, ThematicStatistics, CN_INFO_CONFIG
from ..mongodb import get_db
from ..utils import loop_period_by, sanitize_dates

DT_FMT = r"%Y-%m-%d"
MIN_DIFF_DAYS = 30
MAX_DIFF_DAYS = 91


def get_coll(name):
    db = get_db('cninfo')
    return db[name]


def get_max_dt(coll, code, dt_field):
    """获取表时间字段最大值，通过限定代码可以指定具体股票"""
    # 整表字段最大值
    if dt_field is None:
        dt_field = CN_INFO_CONFIG[coll.name][1]
    pipe = [
        {
            '$sort': {
                dt_field: -1
            }
        },
        {
            '$project': {
                '_id': 0,
                dt_field: 1
            }
        },
        {
            '$limit': 1
        },
    ]
    if code:
        pipe.insert(0, {'$match': {'股票代码': code}})
    try:
        dt = pd.Timestamp(list(coll.aggregate(pipe))[0][dt_field])
        return dt
    except (IndexError, ):
        return pd.Timestamp(CN_INFO_CONFIG[coll.name][0])


def get_start(coll):
    """整表默认时间字段开始刷新日期"""
    now = pd.Timestamp('now').normalize()
    max_dt = get_max_dt(coll, None, None)
    diff = max_dt - now
    min_days = pd.Timedelta(days=MIN_DIFF_DAYS)
    max_days = pd.Timedelta(days=MAX_DIFF_DAYS)
    if min_days <= diff <= max_days:
        return max_dt - pd.Timedelta(days=1)
    else:
        return max_dt + pd.Timedelta(days=1)


def create_index_for(coll):
    name = coll.name
    dt_field = CN_INFO_CONFIG[name][1]
    coll.create_index([("股票代码", 1)], name='code_index')
    coll.create_index([(dt_field, -1)], name='dt_index')
    unique = CN_INFO_CONFIG[name][2]
    if unique:
        coll.create_index([(dt_field, -1), ("股票代码", 1)],
                          unique=True,
                          name='id_index')


def refresh_margin():
    """刷新融资融券数据"""
    name = '融资融券明细'
    coll = get_coll(name)
    with ThematicStatistics() as api:
        if not coll.estimated_document_count() > 0:
            api.logger.info(f"为集合'{name}'创建索引")
            # 创建索引
            create_index_for(coll)
        level = api.name_to_level(name)
        if level != '82':
            api.logger.exception(f"web api 已经更改，退出系统")
            sys.exit()
        # 从当天开始，尝试添加。由于沪深不同步，数据可能存在延时
        # start = get_max_dt(coll, None, None)
        start = get_start(coll)
        end = pd.Timestamp('now')
        # 以时点判断结束日期，昨日或前日
        if end.hour >= 9:
            end = end - pd.Timedelta(days=1)
        else:
            end = end - pd.Timedelta(days=2)
        end = end.normalize()
        ps = loop_period_by(start, end, freq='B')
        for s, e in ps:
            docs = api.get_data(level, s, e)
            count = 0
            for doc in docs:
                try:
                    coll.insert_one(doc)
                    count += 1
                except DuplicateKeyError:
                    pass
            t1_str = s.strftime(DT_FMT)
            t2_str = e.strftime(DT_FMT)
            api.logger.info(
                f"插入集合'{name}'{count:>5}行({t1_str} ~ {t2_str} 保留 {count}/{len(docs)})"
            )


def _refresh(coll, api, t1, t2):
    """数据搜索"""
    name = coll.name
    docs = api.get_data(name, t1, t2)
    dt_field = CN_INFO_CONFIG[coll.name][1]
    to_add = list(filter(lambda x: t1 <= x[dt_field] <= t2, docs))
    if len(to_add):
        coll.insert_many(to_add)
        t1_str = t1.strftime(DT_FMT)
        t2_str = t2.strftime(DT_FMT)
        api.logger.info(
            f"插入集合'{name}'{len(to_add):>5}行({t1_str} ~ {t2_str} 保留 {len(to_add)}/{len(docs)})"
        )


def refresh_asr(items):
    """刷新数据浏览器项目数据"""
    with AdvanceSearcher() as api:
        pos_list = api.pos_list
        for pos in pos_list:
            name = api.level_to_name(pos)
            if name not in items:
                continue
            api.to_level(pos)
            coll = get_coll(name)
            if not coll.estimated_document_count() > 0:
                api.logger.info(f"为集合'{name}'创建索引")
                # 创建索引
                create_index_for(coll)
            # start = get_max_dt(coll, None, None) + pd.Timedelta(days=1)
            # 正确处理财报开始日
            start = get_start(coll)
            end = pd.Timestamp('now').normalize()
            try:
                # 按年循环，含当年
                ps = loop_period_by(start, end, 'Y', False)
            except FutureDate:
                continue
            for s, e in ps:
                _refresh(coll, api, s, e)
