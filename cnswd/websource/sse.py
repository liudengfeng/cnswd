"""
上交所网页
    上市公司列表
        股票
            上市A股
            上市B股
        首次发行待上市股票
        暂停/终止上市公司
    上市公司信息
        上市公司公告
        定期报告
        退市整理期公司公告
        限售股份解限与减持
"""

import random
import time


import logbook
import pandas as pd

from selenium.common.exceptions import NoSuchElementException, ElementNotInteractableException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait

from cnswd.utils import data_root, most_recent_path
from cnswd._seleniumwire import make_headless_browser


logger = logbook.Logger('上交所')

MAX_WAIT_SECOND = 10
CHANNEL_MAPS = {
    'listedNotice_disc': '上市公司公告',
    'fixed_disc': '定期报告',
    'delist_disc': '退市整理期公司公告'
}


class SSEPage(object):
    """上交所Api"""

    def __init__(self, download_path=data_root('download')):
        self.host_url = 'http://www.sse.com.cn'
        logger.info('初始化无头浏览器......')
        self.driver = make_headless_browser()
        self.wait = WebDriverWait(self.driver, MAX_WAIT_SECOND)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.driver.quit()

    def _goto_page(self, num, input_id, btn_id):
        """跳转到指定页数的页面

        Arguments:
            num {int} -- 页数
            input_id {str} -- 输入页码id
            btn_id {str} -- 命令按钮id
        """
        i = self.driver.find_element_by_id(input_id)
        i.clear()
        i.send_keys(num)
        self.driver.find_element_by_id(btn_id).click()

    def _read_table(self, num, stock_type):
        """获取第x页的股票列表信息

        Arguments:
            url {str} -- 网址
            num {int} -- 页码序号

        Returns:
            pd.DataFrame -- 单页股票列表信息
        """
        self._goto_page(num, 'ht_codeinput', 'pagebutton')
        logger.info(f"{stock_type} 第{num}页")
        return pd.read_html(self.driver.page_source, header=0)[0]

    def _get_total_page_num(self, id_name='pagebutton', attr_name='page'):
        """获取分页总数

        Arguments:
            url {str} -- 网址

        Keyworandom.random() Arguments:
            id_name {str} -- 页数所在id名称 (default: {'pagebutton'})
            attr_name {str} -- 页数对应的属性名称 (default: {'page'})

        Returns:
            integer -- 分页数量
        """
        try:
            e = self.driver.find_element_by_id(id_name)
            return int(e.get_attribute(attr_name))
        except NoSuchElementException:
            return 0
        except ElementNotInteractableException:
            return 0

    def _to_datetime(self, df, cols):
        for col in cols:
            df[col] = pd.to_datetime(
                df[col].values, 'coerce', infer_datetime_format=True)
        return df

    def _get_item_data(self, suffix, tab_css, info, date_cols, item=None):
        """获取股票列表信息"""
        url = self.host_url + suffix
        # 严格顺序
        # 1.浏览网页
        self.driver.get(url)
        # 2.等待响应表完成加载
        table_css = '.table > tbody:nth-child(1) > tr > th'
        locator = (By.CSS_SELECTOR, table_css)
        self.wait.until(EC.visibility_of_all_elements_located(locator))
        # 3.选择板块
        drop_css = 'div.single_select2 > button:nth-child(1)'
        label_css_fmt = 'div.single_select2 > div:nth-child(2) > ul:nth-child(1) > li:nth-child({}) > label'
        if item:
            self.driver.find_element_by_css_selector(drop_css).click()
            self.driver.implicitly_wait(0.1)
            self.driver.find_element_by_css_selector(
                label_css_fmt.format(item)).click()
        # 4.转换栏目
        if tab_css is not None:
            self.driver.find_element_by_css_selector(tab_css).click()
        else:
            # 此时使用查询按钮
            btn_css = '#btnQuery'
            self.driver.find_element_by_css_selector(btn_css).click()
        
        # 5.获取页数
        total = self._get_total_page_num()
        # 6.分页读取
        # 如果仅有1页，则不需要循环
        if total in (0, 1):
            return pd.read_html(self.driver.page_source, header=0)[0]
        dfs = []
        for i in range(1, total + 1):
            df = self._read_table(i, info)
            dfs.append(df)
        res = pd.concat(dfs)
        return self._to_datetime(res, date_cols)

    def get_stock_list_a(self):
        """获取A股股票列表信息

        Returns:
            pd.DataFrame -- A股股票列表
        """
        suffix = '/assortment/stock/list/share/'
        tab_css = None
        date_cols = ['上市日期']
        item = 1
        df = self._get_item_data(suffix, tab_css, '上市A股', date_cols, item)
        return df.drop('公告', axis=1)

    def get_stock_list_b(self):
        """获取B股股票列表信息

        Returns:
            pd.DataFrame -- B股股票列表
        """
        suffix = '/assortment/stock/list/share/'
        tab_css = None
        date_cols = ['上市日期']
        item = 2
        df = self._get_item_data(suffix, tab_css, '上市B股', date_cols, item)
        return df.drop('公告', axis=1)

    def get_stock_list_c(self):
        """获取科创板股票列表信息

        Returns:
            pd.DataFrame -- 科创板股票列表
        """
        suffix = '/assortment/stock/list/share/'
        tab_css = None
        date_cols = ['上市日期']
        item = 3
        df = self._get_item_data(suffix, tab_css, '科创板', date_cols, item)
        return df.drop('公告', axis=1)

    def get_firstissue(self):
        """获取首次发行待上市股票信息

        Returns:
            pd.DataFrame -- 首次发行待上市股票
        """
        suffix = '/assortment/stock/list/firstissue/'
        tab_css = None
        date_cols = ['上市日期']
        df = self._get_item_data(suffix, tab_css, '首次发行待上市', date_cols)
        return df.drop('公告', axis=1)

    def get_suspend(self):
        """获取暂停上市公司股票信息

        Returns:
            pd.DataFrame -- 暂停上市公司股票信息
        """
        suffix = '/assortment/stock/list/delisting/'
        tab_css = 'li.active:nth-child(1) > a:nth-child(1)'
        date_cols = ['上市日期', '暂停上市日期']
        df = self._get_item_data(suffix, tab_css, '暂停上市', date_cols)
        if '对不起！找到了0条数据' in df['公司代码']:
            df = pd.DataFrame()
        return df

    def get_delisting(self):
        """获取终止上市公司股票信息

        Returns:
            pd.DataFrame -- 终止上市公司股票
        """
        suffix = '/assortment/stock/list/delisting/'
        tab_css = 'ul.nav:nth-child(1) > li:nth-child(2) > a:nth-child(1)'
        date_cols = ['上市日期', '终止上市日期']
        df = self._get_item_data(suffix, tab_css, '终止上市', date_cols)
        return df

    def get_dividend_a(self):
        """A股分红情况"""
        # TODO:设置年份和代码
        # 当前仅能查询最新分红情况
        suffix = '/market/stockdata/dividends/dividend/'
        tab_css = 'ul.nav:nth-child(1) > li:nth-child(1)'
        date_cols = ['股权登记日', '除息日']
        df = self._get_item_data(suffix, tab_css, 'A股分红', date_cols)
        return df

    def get_dividend_b(self):
        """B股分红情况"""
        # TODO:设置年份和代码
        # 当前仅能查询最新分红情况
        suffix = '/market/stockdata/dividends/dividend/'
        tab_css = 'ul.nav:nth-child(1) > li:nth-child(2)'
        date_cols = ['最后交易日', '股权登记日', '除息日']
        df = self._get_item_data(suffix, tab_css, 'B股分红', date_cols)
        return df

    def get_bonus(self):
        """送股情况 """
        # TODO:设置年份和代码
        # 当前仅能查询最新分红情况
        suffix = '/market/stockdata/dividends/bonus/'
        tab_css = None
        date_cols = ['最后交易日（B股）', '股权登记日', '除权基准日', '红股上市日', '公告刊登日']
        df = self._get_item_data(suffix, tab_css, '送股情况 ', date_cols)
        return df


if __name__ == '__main__':
    with SSEPage() as api:
        # print(api.get_delisting())
        print(api.get_stock_list_b())
