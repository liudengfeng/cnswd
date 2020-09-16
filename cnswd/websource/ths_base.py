import random
import time

import pandas as pd
from retry.api import retry_call
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from .._seleniumwire import make_headless_browser
from ..setting.config import POLL_FREQUENCY, TIMEOUT
from ..utils import make_logger

# import warnings

# warnings.filterwarnings('ignore')
logger = make_logger('同花顺')


def parse_info(elem):
    """解析元素a元素信息"""
    href = elem.get_attribute('href')
    gn_code = href.split('/')[-2]
    return {'概念编码': gn_code, '概念名称': elem.text, 'url': href}


class THS(object):
    """同花顺网页信息api"""
    def __init__(self):
        self.logger = logger
        self.logger.info("创建无头浏览器")
        self.browser = make_headless_browser()
        self.wait = WebDriverWait(self.browser, 60, POLL_FREQUENCY)

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

    def get_gn_detail(self, gn_code, gn_name):
        """股票概念列表信息

        Args:
            gn_code (str)): 概念编码

        Returns:
            list: list of dict
        """
        url = 'http://q.10jqka.com.cn/gn/detail/code/{}/'.format(gn_code)
        self.browser.get(url)
        css = '#maincont'
        m = EC.presence_of_element_located((By.CSS_SELECTOR, css))
        self.wait.until(m, message='加载成分股清单')
        definition_css = '.board-txt > p:nth-child(2)'
        definition = self.browser.find_element_by_css_selector(
            definition_css).text
        num = self._get_page_num()
        target_url_fmt = "http://q.10jqka.com.cn/gn/detail/field/264648/order/desc/page/{}/ajax/1/code/{}"
        codes = []
        for page in range(1, num + 1):
            target_url = target_url_fmt.format(page, gn_code)
            self.browser.get(target_url)
            df = self._read_html_data(self.browser.page_source, gn_code)
            codes.extend(df['股票代码'].values.tolist())
            logger.info(
                f"概念股票列表 {gn_name} {page:>4}/{num:>4} 行数{len(df):>4}")
        return {'概念编码': gn_code, '股票列表': codes, '概念定义': definition}

    def get_gn_urls(self):
        """股票概念网址列表"""
        url = 'http://q.10jqka.com.cn/zjhhy/'
        self.browser.get(url)
        cate_toggle_css = '.cate_toggle'
        locator = (By.CSS_SELECTOR, cate_toggle_css)
        elem = self.wait.until(EC.visibility_of_element_located(locator))
        elem.click()
        self.browser.implicitly_wait(0.3)
        info = self.browser.find_elements_by_xpath(
            '/html/body/div[2]/div[1]//a')
        res = []
        for a in info:
            d = parse_info(a)
            res.append(d)
        return res

    def get_gn_times(self):
        """股票概念概述列表"""
        url = 'http://q.10jqka.com.cn/gn/'
        self.browser.get(url)
        css = 'div.box:nth-child(5) > div:nth-child(1) > h2:nth-child(1)'
        m = EC.presence_of_element_located((By.CSS_SELECTOR, css))
        self.wait.until(m, message='加载概念概述失败')
        num = self._get_page_num()
        target_url_fmt = 'http://q.10jqka.com.cn/gn/index/field/addtime/order/desc/page/{}/ajax/1/'
        res = []
        # 数据框 0 日期 1 概念名称 2 驱动事件
        for page in range(1, num + 1):
            target_url = target_url_fmt.format(page)
            self.browser.get(target_url)
            df = pd.read_html(self.browser.page_source, encoding='gb2312')[0]
            for i in range(len(df)):
                name = df.iat[i, 1]
                elem = self.browser.find_element_by_link_text(name)
                info = parse_info(elem)
                info['日期'] = pd.to_datetime(df.iat[i, 0], errors='coerce')
                info['驱动事件'] = df.iat[i, 2]
                res.append(info)
            logger.info(f"概念简介 {page:>4}/{num:>4} 行数{len(df):>4}")
            time.sleep(random.uniform(0.2, 0.5))
        return res

    def get_concept_info(self):
        infoes = self.get_gn_times()
        for d in infoes:
            gn_code = d['概念编码']
            gn_name = d['概念名称']
            add_info = retry_call(self.get_gn_detail, [gn_code, gn_name],
                                  tries=3,
                                  delay=0.3,
                                  logger=logger)
            d['股票列表'] = add_info['股票列表']
            d['概念定义'] = add_info['概念定义']
        return infoes
