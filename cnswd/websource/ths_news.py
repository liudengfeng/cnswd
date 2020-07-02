import asyncio

import aiohttp
import pandas as pd
# import re
from cnswd.mongodb import get_db
from cnswd.utils import make_logger

logger = make_logger('同花顺财经')

HEADERS = {
    'Host': 'news.10jqka.com.cn',
    'User-Agent':
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language': 'zh-CN,en-US;q=0.7,en;q=0.3',
    'Accept-Encoding': 'gzip, deflate, br',
    'X-Requested-With': 'XMLHttpRequest',
    'Connection': 'keep-alive',
    'Referer': 'https://news.10jqka.com.cn/realtimenews.html',
}

DEFAULT_KWARGS = {'page': 1, 'tag': '', 'track': 'website', 'pagesize': 400}
url = 'https://news.10jqka.com.cn/tapp/news/push/stock/'
# T_PATT = re.compile(".*?time$")
# logger = make_logger('同花顺财经')


def to_timestamp(x):
    # 输入字符串
    dt = pd.Timestamp(int(x), unit='s', tz='Asia/Shanghai')
    return dt.tz_localize(None)


async def fetch_news(page=1):
    """提取指定页的财经消息

    Args:
        page (int, optional): 第几页. Defaults to 1.

    Returns:
        list: 字典列表(适合mongodb)
    """
    kwargs = DEFAULT_KWARGS.copy()
    kwargs.update({'pagesize': page * kwargs['pagesize']})
    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=kwargs, headers=HEADERS) as r:
            # 解析结果
            res = await r.json()
            # dt = to_timestamp(res['time'])
            # logger.info(f"Page {page:>5} : {res['msg']} time {dt}")
            data = res['data']['list']
            for item in data:
                item['id'] = int(item['id'])
                item['seq'] = int(item['seq'])
                item['ctime'] = to_timestamp(item['ctime'])
                item['rtime'] = to_timestamp(item['rtime'])
            data.sort(key=lambda x: x['id'])
            return data
