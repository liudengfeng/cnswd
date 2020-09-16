"""
财务分析
    近三年财务分析

"""
import pandas as pd
from .base import THS

url_fmt = "http://data.10jqka.com.cn/financial/{}/"
titles = ['yjyg', 'yjkb', 'yjgg', 'yypl', 'sgpx', 'ggjy']


class FinancialAnalysis(THS):
    api_name = ''
    date_css = '.text'
    r_date_css = '.list'
    table_attrs = {'class': 'm-table J-ajax-table J-canvas-table'}

    _loaded = False

    title = 'yjyg'
    # 业绩快报、业绩公告需要重新定义列名称
    data_col_names = []  # 数据列名称

    def _load_page(self):
        """加载网页"""
        if self._loaded:
            return
        assert self.title in titles, f"财务分析栏目限定：{titles}"
        current_url = url_fmt.format(self.title)
        self.logger.info(f"加载：{current_url}")
        self.driver.get(current_url)
        request = self.driver.wait_for_request(current_url)
        if request.response.status_code != 200:
            raise ValueError(request.response.reason)
        self._loaded = True

    @property
    def report_dates(self):
        self._load_page()
        # 激活可选日期
        elem = self.driver.find_element_by_css_selector(self.date_css)
        elem.click()
        css = f'{self.r_date_css} a[href]'
        elems = self.driver.find_elements_by_css_selector(css)
        # 关闭显示可选日期
        elem.click()
        return [e.get_attribute('date') for e in elems]

    def _change_report_date(self, date_str):
        assert date_str in self.report_dates
        # 首先激活报告日期选项
        self.driver.find_element_by_css_selector(self.date_css).click()
        elem_css = f'{self.r_date_css} a[date="{date_str}"]'
        elem = self.driver.find_element_by_css_selector(elem_css)
        elem.click()

    def _get_data(self, date_str):
        """指定报告日期数据"""
        # 报告期参数对高管持股无效
        if self.title != 'ggjy':
            self._change_report_date(date_str)
            path = f"date/{date_str}"
            self._wait_path_loading(path, 20)
        else:
            self.random_sleep()
        return self.read_pages_table()

    def get_data(self, date_str=None):
        """获取指定报告期财务分析数据"""
        self._load_page()
        # 报告期参数对高管持股无效
        if self.title != 'ggjy':
            if date_str is None:
                dfs = [self._get_data(d) for d in self.report_dates]
            else:
                dfs = [self._get_data(date_str)]
        else:
            dfs = [self._get_data(date_str)]
        del self.driver.requests
        df = pd.concat(dfs)
        if self.data_col_names:
            df.columns = self.data_col_names
        df.drop(columns=['更多', '历史业绩数据'], inplace=True, errors='ignore')
        return df


class YJYG(FinancialAnalysis):
    api_name = '业绩预告'
    title = 'yjyg'


class YJKB(FinancialAnalysis):
    api_name = '业绩快报'
    title = 'yjkb'
    data_col_names = [
        '序号', '股票代码', '股票简称', '公告日期', '营业收入（元）',
        '营业收入_去年同期（元）', '营业收入_同比增长（%）', '营业收入_季度环比增长（%）',
        '净利润（元）', '净利润_去年同期（元）', '净利润_同比增长（%）', '净利润_季度环比增长（%）',
        '每股收益（元）', '每股净资产（元）', '净资产收益率（%）', '更多'
    ]


class YJGG(FinancialAnalysis):
    api_name = '业绩公告'
    title = 'yjgg'
    data_col_names = [
        '序号', '股票代码', '股票简称', '公告日期', '营业收入（元）',
        '营业收入_同比增长（%）', '营业收入_季度环比增长（%）',
        '净利润（元）', '净利润_同比增长（%）', '净利润_季度环比增长（%）',
        '每股收益（元）', '每股净资产（元）', '净资产收益率（%）',
        '每股经营现金流量（元）', '销售毛利率（%）', '更多'
    ]


class YJYPL(FinancialAnalysis):
    api_name = '业绩预披露'
    title = 'yypl'


class SGPX(FinancialAnalysis):
    api_name = '送股派息'
    title = 'sgpx'


class GGCG(FinancialAnalysis):
    api_name = '高管持股'
    title = 'ggjy'
