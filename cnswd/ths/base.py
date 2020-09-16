"""同花顺基础类

Notes:
    1. 当提取数据失败，使用driver.delete_all_cookies()
"""
import random
import sys
import time

import logbook
import pandas as pd
from retry import retry
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from cnswd._seleniumwire import make_headless_browser
from cnswd.setting.config import POLL_FREQUENCY, TIMEOUT

logbook.StreamHandler(sys.stdout).push_application()
logbook.set_datetime_format("local")
logger = logbook.Logger('同花顺')


class THS(object):
    """同花顺网页信息api"""
    # 子类定义读取表属性
    table_attrs = {}

    def __init__(self):
        self.logger = logger
        self.logger.info("创建无头浏览器")
        self.driver = make_headless_browser()
        self.wait = WebDriverWait(self.driver, 60, POLL_FREQUENCY)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.driver.delete_all_cookies()
        self.driver.quit()

    def _parse_href_in_table(self, step=2):
        """解析表内a元素的网址"""
        # 表内可能有多个链接，需要间隔选取
        # 2 ：√×√×
        # 3 ：√××√××
        css = 'td a[href]'
        ls = self.driver.find_elements_by_css_selector(css)
        res = {}
        for e in ls[::step]:
            res[e.text] = e.get_attribute('href')
        return res

    def read_page_table(self):
        """读取指定属性所在页的表数据"""
        attrs = self.table_attrs
        return pd.read_html(self.driver.page_source, attrs=attrs)[0]

    def read_pages_table(self):
        """连续读取多页表数据"""
        dfs = []
        page_num = self.get_page_num()
        first_df = self.read_page_table()
        if page_num == 1:
            dfs = [first_df]
        else:
            for p in range(2, page_num+1):
                self._change_page_to(p)
                path = f'page/{p}'
                self._wait_path_loading(path)
                df = self.read_page_table()
                dfs.append(df)
            dfs.insert(0, first_df)
        # 由于采用简单路径定位，为防止后续定位误判，读取完成后
        # 务必删除所有请求
        del self.driver.requests
        return pd.concat(dfs)

    def _wait_path_loading(self, path, timeout=10):
        request = self.driver.wait_for_request(path, timeout)
        # self.logger.info(
        #     f"{request.path} 响应状态码：{request.response.status_code}")
        return request

    def get_page_num(self, css='.page_info'):
        """当前数据总页数"""
        try:
            elem = self.driver.find_element_by_css_selector(css)
            return int(elem.text.split('/')[1])
        except Exception:
            return 1

    def _change_page_to(self, page_num):
        """转换到指定页【只能连续转换，不可跳跃】"""
        page_css = f'a[page="{page_num}"]'
        self.driver.find_element_by_css_selector(page_css).click()

    def random_sleep(self):
        time.sleep(random.random())
