"""
腾讯股票数据模块

数据类别：
    腾讯行业分类               fetch_qq_industry_categories
    概念分类                   fetch_concept_categories
    地域分类                    fetch_region_categories
    证监会行业分类              fetch_csrc_industry_categories
    腾讯行业股票列表            fetch_qq_industry_stocks
    概念股票列表                fetch_concept_stocks
    地域股票列表                fetch_region_stocks
    证监会行业股票列表           fetch_csrc_industry_stocks
    股票成交数据（每分钟更新）   fetch_minutely_prices
    最近交易的股票清单           get_recent_trading_stocks
"""

from __future__ import absolute_import, division, print_function

import re
from io import BytesIO

import click
import pandas as pd
from bs4 import BeautifulSoup

from .base import get_page_response

QQ_URL_BASE = 'http://stockapp.finance.qq.com/mstats/'


def _code_to_symbol(stock_code):
    """股票代码转换为查询符号"""
    if stock_code[0] == '6':
        return 'sh{}'.format(stock_code)
    else:
        return 'sz{}'.format(stock_code)


def _page_content(url):
    """页面内容"""
    response = get_page_response(url)
    response.encoding = 'utf-8'
    return response.text


def _fetch_categories(id_pattern, href_pattern):
    """解析类别列表"""
    url = QQ_URL_BASE + '?mod=all'
    text = _page_content(url)
    soup = BeautifulSoup(text, 'lxml')
    targets = soup.find_all('a', 'clk-mo-li',
                            id=id_pattern,
                            href=href_pattern)
    res = [(a['id'], a.string) for a in targets]
    df = pd.DataFrame.from_records(res)
    df.columns = ['id', 'name']
    return df


def fetch_qq_industry_categories():
    """腾讯行业分类"""
    id_p = re.compile(r'a-l-bd01\d{4}')
    href = re.compile(r'id=bd01\d{4}')
    df = _fetch_categories(id_p, href)
    return df


def fetch_concept_categories():
    """概念分类"""
    id_p = re.compile(r'a-l-bd02\d{4}')
    href = re.compile(r'id=bd02\d{4}')
    df = _fetch_categories(id_p, href)
    df.columns = ['concept_id', 'name']
    return df


def fetch_region_categories():
    """地域分类"""
    id_p = re.compile(r'a-l-bd03\d{4}')
    href = re.compile(r'id=bd03\d{4}')
    df = _fetch_categories(id_p, href)
    return df


def fetch_csrc_industry_categories():
    """证监会行业分类"""
    id_p = re.compile(r'a-l-bd04[A-Z]{2}\d{2}')
    href = re.compile(r'bd04[A-Z]{2}\d{2}')
    df = _fetch_categories(id_p, href)
    return df


def _parse_stock_codes(text, has_prefix=False):
    """解析股票代码"""
    if has_prefix:
        pattern = re.compile(r"(s[hz]\d{6})")
    else:
        pattern = re.compile(r"s[hz](\d{6})")
    return re.findall(pattern, text)


def _fetch_one_item_stocks(item_id, item_name):
    """提取单个行业（区域、概念）的股票清单"""
    url_fmt = 'http://stock.gtimg.cn/data/index.php?appn=rank&t=pt{}/chr&l=1000&v=list_data'
    url = url_fmt.format(item_id)
    response = get_page_response(url)
    codes = pd.Series(_parse_stock_codes(response.text)).unique()
    df = pd.DataFrame(
        {'item_id': item_id, 'item_name': item_name, 'code': codes})
    return df


def _fetch_item_stocks(item_data):
    """提取行业（区域、概念）的股票清单"""
    dfs = []
    with click.progressbar(item_data.iterrows(),
                           length=len(item_data),
                           label="提取股票清单") as bar:
        for x in bar:
            item_id = x[1][0][-6:]
            item_name = x[1][1]
            df = _fetch_one_item_stocks(item_id, item_name)
            dfs.append(df)
    df = pd.concat(dfs, sort=True)
    df.reset_index(drop=True, inplace=True)
    return df


def fetch_qq_industry_stocks():
    """腾讯行业股票列表"""
    inds = fetch_qq_industry_categories()
    return _fetch_item_stocks(inds)


def fetch_concept_stocks():
    """概念股票列表"""
    inds = fetch_concept_categories()
    return _fetch_item_stocks(inds)


def fetch_region_stocks():
    """地域股票列表"""
    inds = fetch_region_categories()
    return _fetch_item_stocks(inds)


def fetch_csrc_industry_stocks():
    """证监会行业股票列表"""
    inds = fetch_csrc_industry_categories()
    return _fetch_item_stocks(inds)


def fetch_minutely_prices():
    """所有股票当前成交数据（每分钟更新）"""
    url = 'http://stock.gtimg.cn/data/get_hs_xls.php?id=ranka&type=1&metric=chr'
    kwds = {'skiprows': [0], 'index_col': '代码'}
    page_response = get_page_response(url, 'post')
    df = pd.read_excel(BytesIO(page_response.content), **kwds)
    df.updatetime = pd.Timestamp('now')
    return df


def get_recent_trading_stocks():
    """获取最近一期处于交易状态的股票清单"""
    df = fetch_minutely_prices()
    return sorted(df.query('成交量>0').index.str.slice(2, 8).values)
