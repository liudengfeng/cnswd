"""

深证信专题统计模块

作为数据提取工具，专题统计的大部分项目没有实际意义，只是对数据进行的统计加工。
有效栏目如`股票状态`等数据搜索未出现的部分

备注：
    当前只用到融资融券(8.2)，其余部分并未测试。
"""

import random
import time

import pandas as pd
from retry.api import retry_call
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select

from ..utils import sanitize_dates
from .base_driver import INTERVAL, SZXPage
from .ops import datepicker
from .utils import cleaned_data


class ThematicStatistics(SZXPage):
    """深证信专题统计api"""
    api_name = '专题统计'
    api_ename = 'thematicStatistics'
    query_btn_css = 'button.stock-search:nth-child(11)'
    delay = 1

    def _is_none(self):
        # 无过滤条件
        css = 'div.tables-filter:nth-child(1)'
        elem = self.driver.find_element_by_css_selector(css)
        return not self._is_view(elem)

    def _is_D(self):
        # 单日期过滤
        css = 'div.filter-condition:nth-child(5)'
        elem = self.driver.find_element_by_css_selector(css)
        return self._is_view(elem)

    def _current_period_type(self):
        """项目日期设置类型"""
        # 根据输入参数有关日期部分决定日期设置行为
        if self._is_none():
            return None
        elif self._is_D():
            return 'DD'
        # elif self._is_QQ():
        #     return 'QQ'
        # elif self._is_YY():
        #     return 'YY'
        raise ValueError("期间类型可能设置错误")

    def _set_date_filter(self, t1, t2):
        period_type = self._current_period_type()
        if period_type is None:
            return
        # 开始日期 ~ 结束日期
        # 默认以季度循环
        if period_type == 'DD':
            css_1 = "#fBDatepair > input:nth-child(1)"
            datepicker(self, t1, css_1)

        # if period_type == 'QD':
        #     css_1 = "input.date:nth-child(1)"
        #     css_2 = "input.form-control:nth-child(2)"
        #     datepicker(self, t1, css_1)
        #     datepicker(self, t2, css_2)
        # # 年
        # if period_type == 'YY':
        #     # 特殊
        #     select_year(self, t1, 'se2')
        # # 年 季度
        # if period_type == 'QQ':
        #     select_year(self, t1)
        #     select_quarter(self, t2)

    def _before_read(self):
        """等待数据完成加载"""
        msg = f"等待查询{self.current_item}响应超时"
        css = '.fixed-table-loading'
        locator = (By.CSS_SELECTOR, css)
        self.wait.until(EC.invisibility_of_element_located(locator), msg)

    def get_data(self, level, start=None, end=None):
        """获取项目数据
        与快速搜索不同，高级搜索限定为当前全部股票

        Arguments:
            level {str} -- 项目层级

        Keyword Arguments:
            start {str} -- 开始日期 (default: {None})，如为空，使用市场开始日期
            end {str} -- 结束日期 (default: {None})，如为空，使用当前日期

        Usage:
            >>> api = AdvanceSearcher()
            >>> api.get_data('82', '2018-01-01','2018-08-10')
            >>> api.driver.quit()

        Returns:
            list -- 数据字典列表
        """
        start, end = sanitize_dates(start, end)
        self.ensure_init()
        self.to_level(level)
        # 首先需要解析元数据
        meta = self.get_level_meta_data(level)
        field_maps = meta['field_maps']
        # 跳转到数据栏目会附带默认参数的遗留数据，为确保数据干净，先删除请求
        # 注意次序。必须先解析完元数据，然后再删除
        del self.driver.requests
        # 当前数据项目中文名称
        self.current_item = meta['api_name']
        data = self._read_data_by_period(start, end)
        return cleaned_data(data, field_maps)
