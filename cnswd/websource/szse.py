"""
深交所网页
    上市公司列表
        上市公司
        暂停上市
        终止上市
        全称变更
        简称变更
    上市公司信息
        上市公司公告
        定期报告
        退市整理期公司公告
        限售股份解限与减持
"""
import random

import pandas as pd

HOST_URL = 'http://www.szse.cn/api/report/ShowReport'


def fetch_companys_info():
    """获取当前在市的股票概要信息
    
    Returns:
        pd.DataFrame -- 股票信息列表
    """
    url = HOST_URL + \
        f'?SHOWTYPE=xlsx&CATALOGID=1110x&TABKEY=tab1?random={random.random()}'
    df = pd.read_excel(url, thousands=',', dtype={
                       '公司代码': str, 'A股代码': str, 'B股代码': str})
    df.columns = df.columns.str.replace(r'\s', '')
    return df


def fetch_suspend_stocks():
    """获取暂停上市股票列表"""
    url = HOST_URL + \
        f'?SHOWTYPE=xlsx&CATALOGID=1793_ssgs&TABKEY=tab1&random={random.random()}'
    df = pd.read_excel(url, dtype={'证券代码': str})
    return df


def fetch_delisting_stocks():
    """获取终止上市股票清单"""
    url = HOST_URL + \
        f'?SHOWTYPE=xlsx&CATALOGID=1793_ssgs&TABKEY=tab2&random={random.random()}'
    df = pd.read_excel(url, dtype={'证券代码': str})
    return df


def fetch_fullname_history():
    """获取全称变更历史"""
    url = HOST_URL + \
        f'?SHOWTYPE=xlsx&CATALOGID=SSGSGMXX&TABKEY=tab1&random={random.random()}'
    df = pd.read_excel(url, dtype={'证券代码': str})
    return df


def fetch_shortname_history():
    """获取简称变更历史"""
    url = HOST_URL + \
        f'?SHOWTYPE=xlsx&CATALOGID=SSGSGMXX&TABKEY=tab2&random={random.random()}'
    df = pd.read_excel(url, dtype={'证券代码': str})
    return df


if __name__ == '__main__':
    print(fetch_companys_info())
