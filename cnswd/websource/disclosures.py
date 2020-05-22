"""
上市公司公告查询

来源：[巨潮资讯网](http://www.cninfo.com.cn/new/commonUrl?url=disclosure/list/notice-sse#)

备注
    使用实际公告时间
    如查询公告日期为2018-12-15 实际公告时间为2018-12-14 16：00：00
"""

import asyncio
import math
import time
import random
import aiohttp
import pandas as pd
import requests
from aiohttp.client_exceptions import ContentTypeError

from cnswd.utils import data_root, make_logger, safety_exists_pkl

# sem = asyncio.Semaphore(10)
logger = make_logger('巨潮公司公告')

URL = 'http://www.cninfo.com.cn/new/hisAnnouncement/query'
COLUMNS = ['序号', '股票代码', '股票简称', '公告标题', '公告时间', '下载网址']

HEADERS = {
    'Host': 'www.cninfo.com.cn',
    'User-Agent':
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:63.0) Gecko/20100101 Firefox/63.0',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language':
    'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
    'Accept-Encoding': 'gzip, deflate',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'X-Requested-With': 'XMLHttpRequest',
    'Connection': 'Keep-Alive',
    'Referer':
    'http://www.cninfo.com.cn/new/commonUrl?url=disclosure/list/notice',
}

CATEGORIES = {
    '全部': None,
    '年报': 'category_nbbg_szsh',
    '半年报': 'category_bndbg_szsh',
    '一季报': 'category_yjdbg_szsh',
    '三季报': 'category_sjdbg_szsh',
    '业绩预告': 'category_yjygjxz_szsh',
    '权益分派': 'category_qyfpxzcs_szsh',
    '董事会': 'category_dshgg_szsh',
    '监事会': 'category_jshgg_szsh',
    '股东大会': 'category_gddh_szsh',
    '日常经营': 'category_rcjy_szsh',
    '公司治理': 'category_gszl_szsh',
    '中介报告': 'category_zj_szsh',
    '首发': 'category_sf_szsh',
    '增发': 'category_zf_szsh',
    '股权激励': 'category_gqjl_szsh',
    '配股': 'category_pg_szsh',
    '解禁': 'category_jj_szsh',
    '债券': 'category_zq_szsh',
    '其他融资': 'category_qtrz_szsh',
    '股权变动': 'category_gqbd_szsh',
    '补充更正': 'category_bcgz_szsh',
    '澄清致歉': 'category_cqdq_szsh',
    '风险提示': 'category_fxts_szsh',
    '特别处理和退市': 'category_tbclts_szsh',
}

PLATES = {'sz': ('szse', '深市'), 'shmb': ('sse', '沪市')}


def _get_total_record_num(data):
    """公告总数量"""
    return math.ceil(int(data['totalRecordNum']) / 30)


def _to_dataframe(data):
    def f(page_data):
        res = []
        for row in page_data['announcements']:
            to_add = (
                row['announcementId'],
                row['secCode'],
                row['secName'],
                row['announcementTitle'],
                pd.Timestamp(row['announcementTime'], unit='ms'),
                'http://www.cninfo.com.cn/' + row['adjunctUrl'],
            )
            res.append(to_add)
        df = pd.DataFrame.from_records(res, columns=COLUMNS)
        df['公告时间'] = pd.to_datetime(df['公告时间'])
        df['股票代码'] = df['股票代码'].map(lambda x: str(x).zfill(6))
        return df

    dfs = []
    for page_data in data:
        try:
            dfs.append(f(page_data))
        except Exception:
            pass
    return pd.concat(dfs, sort=True)


async def _fetch_disclosure_async(sem, session, plate, category, date_str,
                                  page):
    assert plate in PLATES.keys(), f'可接受范围{PLATES}'
    assert category in CATEGORIES.keys(), f'可接受分类范围：{CATEGORIES}'
    market = PLATES[plate][1]
    sedate = f"{date_str}+~+{date_str}"
    kwargs = dict(
        tabName='fulltext',
        seDate=sedate,
        category=CATEGORIES[category],
        plate=plate,
        column=PLATES[plate][0],
        pageNum=page,
        pageSize=30,
    )
    delay = random.randint(20, 60) / 100
    await asyncio.sleep(delay)
    async with sem:
        # 如果太频繁访问，容易导致关闭连接
        async with session.post(URL, data=kwargs, headers=HEADERS) as r:
            msg = f"{market} {date_str} 第{page}页 响应状态：{r.status}"
            logger.info(msg)
            # await asyncio.sleep(1)
            try:
                return await r.json()
            except ContentTypeError:
                return {}


async def _fetch_one_day(sem, session, plate, date_str):
    """获取深交所或上交所指定日期所有公司公告"""
    data = await _fetch_disclosure_async(sem, session, plate, '全部', date_str,
                                         1)
    page_num = _get_total_record_num(data)
    if page_num == 0:
        return pd.DataFrame()
    logger.notice(f"{PLATES[plate][1]} {date_str} 共{page_num}页", page_num)
    tasks = []
    for i in range(page_num):
        tasks.append(
            _fetch_disclosure_async(sem, session, plate, '全部', date_str,
                                    i + 1))
    # Schedule calls *concurrently*:
    data = await asyncio.gather(*tasks)
    return _to_dataframe(data)


async def fetch_one_day(date):
    """获取指定日期公司所有公告"""
    date = pd.Timestamp(date)
    date_str = date.strftime(r'%Y-%m-%d')
    sem = asyncio.BoundedSemaphore(8)
    async with aiohttp.ClientSession() as session:
        tasks = [
            _fetch_one_day(sem, session, plate, date_str)
            for plate in PLATES.keys()
        ]
        dfs = await asyncio.gather(*tasks)
        if any([not df.empty for df in dfs]):
            # 按序号排列
            return pd.concat(dfs, sort=True).sort_values('序号')
        else:
            return pd.DataFrame(columns=COLUMNS)
