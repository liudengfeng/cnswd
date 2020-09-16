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
from .css import TsCss
from .ops import datepicker, parse_meta_data, read_json_data
from .utils import cleaned_data


class ThematicStatistics(SZXPage):
    """深证信专题统计api"""
    api_name = '专题统计'
    api_ename = 'thematicStatistics'
    css = TsCss
    delay = 1  # 专题统计页加载时传送大量数据，稍微延时

    def get_current_period_type(self, level):
        # 只处理融资融券数据
        return 'BD'

    def _set_date_filter(self, level, t1, t2):
        period_type = self.get_current_period_type(level)
        # if period_type is None:
        #     return
        # 开始日期 ~ 结束日期
        # 默认以季度循环
        if period_type == 'BD':
            datepicker(self, t1, self.css.sdate, use_tab=False)
            self._filter_pattern['tdate'] = str(t1)

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
    # def _read_branch_data(self, level, t1, t2):
    #     """读取分部数据"""
    #     self._set_date_filter(level, t1, t2)
    #     # 点击查询
    #     btn_css = self.css.query_btn
    #     btn = self.driver.find_element_by_css_selector(btn_css)
    #     # 专题统计中部分项目隐藏命令按钮
    #     # if btn.is_displayed():
    #     btn.click()
    #     self._before_read()
    #     res = read_json_data(self, level)
    #     return res

    def _before_read(self):
        """等待数据完成加载"""
        # msg = f"等待查询{self.current_item}响应超时"
        # locator = (By.CSS_SELECTOR, self.css.data_loaded)
        # self.wait.until(EC.invisibility_of_element_located(locator), msg)
        # 暂时无有效方式确定是否完成加载
        self.driver.implicitly_wait(2)

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
        # 首先需要解析元数据
        meta = self.get_level_meta_data(level)
        menu = self.to_level(level)
        assert menu.pos == level, "菜单位置不正确"
        field_maps = meta['field_maps']

        # 当前数据项目中文名称
        self.current_item = meta['api_name']
        data = self._read_data_by_period(level, start, end)
        return cleaned_data(data, field_maps)
