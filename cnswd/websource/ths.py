"""

同花顺数据模块

可能存在防爬策略：禁用一分钟。

ubuntun操作系统中selenium配置过程
# 安装geckodriver
1. 下载geckodriver对应版本 网址：https://github.com/mozilla/geckodriver/releases
2. 解压：tar -xvzf geckodriver*
3. $sudo mv ./geckodriver /usr/bin/geckodriver
"""
import random
import time

import logbook
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from .._seleniumwire import make_headless_browser
from ..setting.config import DB_CONFIG, POLL_FREQUENCY, TIMEOUT
from ..utils import make_logger

log = make_logger('同花顺')


class THS(object):
    """同花顺网页信息api"""
    def __init__(self):
        self.browser = make_headless_browser()
        self.wait = WebDriverWait(self.browser, TIMEOUT, POLL_FREQUENCY)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.browser.quit()

    def _get_page_num(self):
        """当前页数"""
        try:
            p = self.browser.find_element_by_css_selector(
                '.page_info').text.split('/')[1]
            return int(p)
        except Exception:
            return 1

    def _read_html_data(self, body, gn_code):
        df = pd.read_html(body,
                          encoding='gb2312',
                          attrs={'class': 'm-table m-pager-table'})[0]
        df['概念编码'] = gn_code
        df['股票代码'] = df.代码.map(lambda x: str(x).zfill(6))
        return df.loc[:, ['概念编码', '股票代码']]

    def get_gn_detail(self, gn_code):
        url = 'http://q.10jqka.com.cn/gn/detail/code/{}/'.format(gn_code)
        self.browser.get(url)
        css = '#maincont'
        m = EC.presence_of_element_located((By.CSS_SELECTOR, css))
        self.wait.until(m, message='加载成分股清单')
        # if gn_code in ('308539', '300900', '301636','301636'):
        #     time.sleep(0.5)
        num = self._get_page_num()
        dfs = []
        for page in range(num):
            if page != 0:
                self.browser.find_element_by_link_text('下一页').click()
                requests = self.browser.requests
                path = [r.path for r in requests if gn_code in r.path][0]
                r = self.browser.wait_for_request(path)
                df = self._read_html_data(r.response.body, gn_code)
            else:
                df = self._read_html_data(self.browser.page_source, gn_code)
            dfs.append(df)
            log.notice(f"{page+1}/{num} 行数{len(df)}")
            del self.browser.requests
            time.sleep(0.3)
        return pd.concat(dfs, sort=True)

    @property
    def gn_urls(self):
        """股票概念网址列表"""
        url = 'http://q.10jqka.com.cn/gn/'
        self.browser.get(url)
        self.browser.find_element_by_css_selector('.cate_toggle').click()
        time.sleep(0.5)
        url_css = '.category a'
        info = self.browser.find_elements_by_css_selector(url_css)
        res = []
        for a in info:
            res.append((a.get_attribute('href'), a.text))
        return res

    def _gn_times(self, body, page):
        na_values = ['--', '无']
        df = pd.read_html(body, encoding='gb2312', na_values=na_values)[0]
        return df

    @property
    def gn_times(self):
        """股票概念概述列表"""
        url = 'http://q.10jqka.com.cn/gn/'
        self.browser.get(url)
        num = self._get_page_num()
        dfs = []
        for page in range(num):
            if page != 0:
                self.browser.find_element_by_link_text('下一页').click()
                requests = self.browser.requests
                path = [
                    r.path for r in requests
                    if 'gn/index/field/addtime' in r.path
                ][0]
                r = self.browser.wait_for_request(path)
                df = self._gn_times(r.response.body, page + 1)
            else:
                df = self._gn_times(self.browser.page_source, 1)
            dfs.append(df)
            log.notice(f"{page+1}/{num} 行数{len(df)}")
            del self.browser.requests
            time.sleep(0.3)
        res = pd.concat(dfs)
        res.columns = ['日期', '概念名称', '驱动事件', '龙头股', '成分股数量']
        return res
