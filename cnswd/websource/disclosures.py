"""
上市公司公告查询

来源：[巨潮资讯网](http://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search&startDate=2020-06-23&endDate=2020-07-24#szse/1)

备注
    简化为查询深沪期间所有类型的公告
"""
import asyncio
import math
import random
import time

import aiohttp
import pandas as pd
from aiohttp.client_exceptions import ContentTypeError

from ..setting.constants import MAX_WORKER
from ..utils import make_logger

PAGE_SIZE = 30
logger = make_logger('公司公告')

URL = 'http://www.cninfo.com.cn/new/hisAnnouncement/query'
DOWNLOAD_URL = 'http://static.cninfo.com.cn/'
# 只选择 公司公告、预披露
COLUMNS = {"szse": "深沪", "pre_disclosure": "预披露"}

HEADERS = {
    'Host': 'www.cninfo.com.cn',
    'User-Agent':
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language': 'zh-CN,en-US;q=0.7,en;q=0.3',
    'Accept-Encoding': 'gzip, deflate',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'X-Requested-With': 'XMLHttpRequest',
    'Connection': 'Keep-Alive',
    'Referer': 'http://www.cninfo.com.cn/new/commonUrl/pageOfSearch',
}


def get_total_page_num(data):
    """总页数"""
    return math.ceil(int(data['totalRecordNum']) / PAGE_SIZE)


async def _fetch_disclosure_async(sem,
                                  session,
                                  column,
                                  start_str,
                                  end_str,
                                  page,
                                  total=None):
    sedate = f"{start_str}+~+{end_str}"
    kwargs = dict(
        tabName='fulltext',
        seDate=sedate,
        column=column,
        pageNum=page,
        pageSize=PAGE_SIZE,
        isHLtitle='true',
    )
    delay = random.randint(20, 60) / 100
    await asyncio.sleep(delay)
    info_type = COLUMNS[column]
    async with sem:
        # 如果太频繁访问，容易导致关闭连接
        async with session.post(URL, data=kwargs, headers=HEADERS) as r:
            msg = f"{info_type:>6} {start_str} to {end_str} page {page:>3}"
            if total:
                msg += f" / {total}"
            logger.info(msg)
            try:
                return await r.json()
            except ContentTypeError:
                return {}


async def _fetch_disclosure(sem, session, column, start_str, end_str):
    """获取深交所或上交所指定日期所有公司公告"""
    data = await _fetch_disclosure_async(sem, session, column, start_str,
                                         end_str, 1)
    total_page = get_total_page_num(data)
    if total_page == 0:
        return {}
    info_type = COLUMNS[column]
    logger.info(
        f"{info_type:>6} from {start_str} to {end_str} total pages {total_page:>3}"
    )
    tasks = []
    for i in range(total_page):
        tasks.append(
            _fetch_disclosure_async(sem, session, column, start_str, end_str,
                                    i + 1, total_page))
    # Schedule calls *concurrently*
    return await asyncio.gather(*tasks)


async def fetch_disclosure(start, end):
    """期间沪深二市所有类型的公司公告

    Args:
        start (date like): 开始日期
        end (date like): 结束日期

    Returns:
        list: list of dict
    """
    start, end = pd.Timestamp(start), pd.Timestamp(end)
    start_str = start.strftime(r'%Y-%m-%d')
    end_str = end.strftime(r'%Y-%m-%d')
    sem = asyncio.BoundedSemaphore(MAX_WORKER)
    tasks = []
    async with aiohttp.ClientSession() as session:
        for column in COLUMNS.keys():
            tasks.append(
                _fetch_disclosure(sem, session, column, start_str, end_str))
        data = await asyncio.gather(*tasks)
        res = []
        for d in data:
            res.extend(parse_data(d))
        return res


def to_timestamp(x):
    dt = pd.Timestamp(x, unit='ms', tz='Asia/Shanghai')
    return dt.tz_localize(None)


def parse_data(data):
    """提取整理数据"""
    announcements = []
    for d in data:
        try:
            announcements.extend(d['announcements'])
        except KeyError:
            pass
    data = []
    for row in announcements:
        new_row = {}
        new_row['公告编号'] = int(row.pop('announcementId'))
        new_row['公告时间'] = to_timestamp(row.pop('announcementTime'))
        new_row['股票代码'] = row.pop('secCode')
        new_row['股票简称'] = row.pop('secName')
        new_row['标题'] = row.pop('announcementTitle')
        new_row['下载网址'] = DOWNLOAD_URL + row.pop('adjunctUrl')
        new_row['公告类型'] = row.pop('announcementType', "").split("||")
        new_row['页类型'] = row.pop('pageColumn')
        data.append(new_row)
    data.sort(key=lambda x: x['公告编号'])
    return data
