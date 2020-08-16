"""新浪24*7财经新闻
"""
import time
from concurrent.futures.thread import ThreadPoolExecutor
from functools import partial

import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from .._seleniumwire import make_headless_browser
from ..setting.config import POLL_FREQUENCY, TIMEOUT
from ..setting.constants import MAX_WORKER
from ..utils import make_logger

logger = make_logger('新浪财经新闻')

TOPIC_MAPS = {
    2: 'A股',
    3: '宏观',
    4: '行业',
    5: '公司',
    6: '数据',
    7: '市场',
    8: '观点',
    9: '央行',
    10: '其他',
    1: '全部',  # 不能分类的，可能在全部显示。为提取消息分类，将其放最后
}

COLUMNS = ['序号', '时间', '概要', '分类']


class Sina247News(object):
    def __init__(self):
        logger.info("生成无头浏览器")
        self.base_url = 'http://finance.sina.com.cn/7x24/'
        self.driver = make_headless_browser()
        self.wait = WebDriverWait(self.driver, 5, POLL_FREQUENCY)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.driver.quit()

    def scrolling(self):
        # 每次递增大约20条
        self.driver.execute_script(
            "window.scrollTo(0, document.body.scrollHeight);")

    def turn_off(self):
        """关闭声音提醒及自动更新"""
        csses = ['.soundswitch', '#autorefresh']
        for css in csses:
            elem = self.driver.find_element_by_css_selector(css)
            if elem.is_selected():
                elem.click()

    def _parse(self, div, tag):
        """解析单个消息内容"""
        # 编号、日期(20180901)、时间('22:31:44')、概要
        ps = div.find_elements_by_tag_name('p')
        dt = f"{div.get_attribute('data-time')} {ps[0].text}"
        fmt_str = r'%Y-%m-%d %H:%M:%S.%f'
        dt = pd.to_datetime(dt, format=fmt_str)
        return {
            '序号': int(div.get_attribute('data-id')),
            '时间': dt,
            '分类': TOPIC_MAPS[tag],
            '概要': ps[1].text,
        }

    def _is_view(self, elem):
        style = elem.get_attribute('style')
        if 'none' in style:
            return False
        elif 'inline-block' in style:
            return True

    def _no_data(self):
        css = '#liveList01_empty'
        elem = self.driver.find_element_by_css_selector(css)
        return elem.is_displayed()

    def _wait_loading(self):
        css = '#liveList01_loading'
        locator = (By.CSS_SELECTOR, css)
        m = EC.invisibility_of_element_located(locator)
        self.wait.until(m, "加载消息超时")

    def _get_topic_news(self, tag, times):
        """获取分类消息"""
        url = self.base_url
        self.driver.get(url)
        self.driver.implicitly_wait(0.1)
        css = f'span.bd_topic:nth-child({tag}) > a:nth-child(1)'
        elem = self.driver.find_element_by_css_selector(css)
        elem.click()
        div_css = 'div.bd_i'
        for i in range(times):
            self.turn_off()
            self.scrolling()
            self._wait_loading()
            if self._no_data():
                break
            logger.info(f'当前栏目：{TOPIC_MAPS[tag]:>6} 第{i+1:>4}页')
        # 滚动完成后，并行读取div元素
        divs = self.driver.find_elements_by_css_selector(div_css)
        # docs = [self._parse(div, tag) for div in divs]
        func = partial(self._parse, tag=tag)
        logger.info('开始解析')
        with ThreadPoolExecutor(MAX_WORKER) as pool:
            docs = pool.map(func, divs)
        logger.info('完成解析')
        del divs
        return list(docs)

    def yield_history_news(self, pages):
        """历史财经新闻(一次性)"""
        for tag in TOPIC_MAPS.keys():
            # yield from self._get_topic_news(tag, pages)
            yield self._get_topic_news(tag, pages)
