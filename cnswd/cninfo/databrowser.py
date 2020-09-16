"""
初始化无头浏览器 约 12s
"""
import random
import re
import time

import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from ..utils import sanitize_dates
from .base_driver import INTERVAL, SZXPage
from .css import DbCss
from .ops import (datepicker, element_attribute_change_to,
                  element_text_change_to, get_db_date_filter_mode, input_code, toggler_market_open,
                  parse_response, select_quarter, select_year, simulated_click)
from .utils import cleaned_data

LEVEL_PAT = re.compile(r"\d{1,3}|[a-z]\d[a-z1-9]")
PAGE_PAT = re.compile("共(.*?)条")


def level_like(x):
    # 项目层级仅仅包含数字和小写字符
    # 此处简单判断是否为层级
    # 否则视同为项目中文名称
    return LEVEL_PAT.match(x) is not None


class DataBrowser(SZXPage):
    """深证信数据浏览器基础类"""
    api_name = '数据浏览器'
    api_ename = 'dataBrowse'
    css = DbCss
    delay = 0
    tab_id = 1

    def nav_tab(self, nth):
        """选择快速搜索或高级搜索

        Args:
            nth (int): 1代表快速搜索，2代表高级搜索
        """
        assert nth in (1, 2)
        current_tab = getattr(self, 'current_tab', None)
        if current_tab == nth:
            return
        css = "btn{}".format(nth)
        btn = self.wait.until(EC.element_to_be_clickable((By.ID, css)))
        btn.click()
        self.current_tab = nth

    def get_current_period_type(self, level):
        """期间内部循环类型"""
        # 根据输入参数有关日期部分决定日期设置行为
        # 第一个字符代表freq
        # freq D | M | Q | Y
        # 第二个字符代表日期格式
        # 表达式 Y: 2019 | Q 2019 1 | D 2019-01-11
        level_name = self.level_to_name(level)
        mode = get_db_date_filter_mode(self, level)
        tab_id = self.tab_id
        if mode == 'DD':
            # 网站设定限制，每次返回最大20000行
            # 数量量大的项目，每次取一个月的数据
            # 由于可能存在网站菜单栏目变动问题，映射表可能发生变化
            # 当前硬编码将`投资评级`设定为按月循环，数据量大约在10000行以内
            # 至于其他栏目甚至可以一次性读取，此处简化为统一按季度内部循环
            if tab_id == 2 and level_name in ('投资评级', ):
                return 'MD'
            # 否则按季度循环
            return 'QD'
        return mode

    def _set_date_filter(self, level, t1, t2):
        mode = self.get_current_period_type(level)
        if mode is None:
            return
        dt_fmt = mode[1]
        # 开始日期 ~ 结束日期
        if dt_fmt == 'D':
            if t1:
                datepicker(self, t1, self.css.sdate)
                self._filter_pattern['sdate'] = str(t1)
            if t2:
                datepicker(self, t2, self.css.edate)
                self._filter_pattern['edate'] = str(t2)
        # 年 季度
        if t1 and dt_fmt == 'Q':
            select_year(self, t1)
            select_quarter(self, t2)
            t = pd.Period(year=t1, quarter=t2, freq='Q').asfreq('D')
            self._filter_pattern['sdate'] = t.strftime(r'%Y%m%d')
            self._filter_pattern['edate'] = t.strftime(r'%Y%m%d')
        # 年
        if t1 and dt_fmt == 'Y':
            # 特殊
            css = '#se2_sele'
            select_year(self, t1, css)
            self._filter_pattern['syear'] = str(t1)

    def _before_read(self):
        locator = (By.CSS_SELECTOR, self.css.data_loaded)
        self.wait.until(element_attribute_change_to(
            locator, 'class', 'onloading hide'))


class FastSearcher(DataBrowser):
    """
    搜索单个股票期间项目信息[深证信-数据浏览器-快速搜索]

    Usage:
        >>> api = FastSearch()
        >>> api.get_data('个股报告期利润表','000333','2018-01-01','2019-12-31')
        >>> api.driver.quit()
    """
    api_name = '数据浏览器-快速搜索'
    tab_id = 1
    delay = 0

    def get_data(self, level_or_name, code, start=None, end=None):
        """获取项目数据

        Arguments:
            level_or_name {str} -- 项目层级或层级全称
            code {str} -- 股票代码

        Keyword Arguments:
            start {str} -- 开始日期 (default: {None})，如为空，使用市场开始日期
            end {str} -- 结束日期 (default: {None})，如为空，使用当前日期

        Returns:
            list -- 数据字典列表
        """
        self.ensure_init()
        self.nav_tab(self.tab_id)
        if not level_like(level_or_name):
            level = self.name_to_level(level_or_name)
        else:
            level = level_or_name
        start, end = sanitize_dates(start, end)
        self.to_level(level)
        # 首先需要解析元数据
        meta = self.get_level_meta_data(level)
        field_maps = meta['field_maps']
        # 当前数据项目中文名称
        self.current_item = meta['api_name']
        input_code(self, code)
        self._filter_pattern['scode'] = code
        data = self._read_data_by_period(level, start, end)
        return cleaned_data(data, field_maps)


class AdvanceSearcher(DataBrowser):
    """搜索全部股票期间项目信息[深证信-数据浏览器-高级搜索]

    Usage:
        >>> api = AdvanceSearcher()
        >>> api.get_data('个股报告期利润表','2018-01-01','2019-12-31')
        >>> api.driver.quit()    
    """
    api_name = '数据浏览器-高级搜索'
    tab_id = 2
    delay = 0

    @property
    def stocks_in_trading(self):
        """全市场股票代码"""
        self.ensure_init()
        # 用时约3秒
        codes = getattr(self, '_codes', {})
        # 由于加载成功后会缓存在本地，后续加载会很快完成
        # 作为一个副产品，可以用于判断市场股票数量的依据
        nums = getattr(self, '_nums', {})
        if not nums:
            data_ids = ['101', '102', '107', '108', '110', '106', '109']
            for data_id in data_ids:
                elem_css = self.css.market_code_fmt.format(data_id)
                api_elem = self.driver.find_element_by_css_selector(elem_css)
                toggler_market_open(self, data_id)
                # self.driver.implicitly_wait(INTERVAL)
                data_name = api_elem.get_attribute('data-name')
                data_api = api_elem.get_attribute('data-api')
                # data_param = api_elem.get_attribute('data-param')
                request = self.driver.wait_for_request(data_api)
                data = parse_response(self, request)
                num = len(data)
                nums[data_id] = num
                self.logger.info(f"{data_name}(股票数量：{num})")
                if num:
                    codes.update({d['SECCODE']: d['SECNAME'] for d in data})
                # 删除请求
                del self.driver.requests
            self._nums = nums
            self._codes = codes
        return self._codes

    def _load_all_code(self):
        """加载全部股票代码"""
        code_loaded = getattr(self, '_code_loaded', False)
        if code_loaded:
            return
        # 调用属性，确保生成数量字典
        _ = self.stocks_in_trading
        locator = (By.CSS_SELECTOR, self.css.to_select_code)
        data_ids = ['101', '102', '107', '108', '110', '106', '109']
        for data_id in data_ids:
            elem_css = self.css.market_code_fmt.format(data_id)
            api_elem = self.driver.find_element_by_css_selector(elem_css)
            # 直接使用模拟点击，简化操作
            simulated_click(self, api_elem)
            # 模拟点击后务必预留时间
            self.driver.implicitly_wait(INTERVAL)
            num = self._nums[data_id]
            text = str(num)
            self.wait.until(element_text_change_to(locator, text))
            # 仅当数量不为0时执行添加
            if self._nums[data_id]:
                # 全部添加
                self._add_or_delete_all(
                    self.css.all_input_code, self.css.add_all_code_btn)
        actual = int(self.driver.find_element_by_css_selector(
            self.css.selected_code).text)
        expected = sum(self._nums.values())
        assert actual == expected, f"股票总数应为：{expected}，实际：{actual}"
        self._code_loaded = True

    def _select_all_fields(self):
        """全选字段"""
        # 全选数据字段
        self._add_or_delete_all(self.css.add_all_field,
                                self.css.add_all_field_btn)

    def _add_or_delete_all(self, label_css, btn_css):
        """添加或删除所选全部元素"""
        # 点击全选元素
        self.driver.find_element_by_css_selector(label_css).click()
        # 点击命令按钮
        self.driver.find_element_by_css_selector(btn_css).click()

    def _before_query(self):
        """执行查询前应完成的动作"""
        # 高级搜索需要加载全部代码、全选查询字段
        self._load_all_code()
        num = int(self.driver.find_element_by_css_selector(
            self.css.selected_field).text)
        # 只有非0时才需要全选字段
        if not num:
            self._select_all_fields()

    def get_data(self, level_or_name, start=None, end=None):
        """获取项目数据
        与快速搜索不同，高级搜索限定为当前全部股票

        Arguments:
            level_or_name {str} -- 项目层级或层级全称

        Keyword Arguments:
            start {str} -- 开始日期 (default: {None})，如为空，使用市场开始日期
            end {str} -- 结束日期 (default: {None})，如为空，使用当前日期

        Usage:
            >>> api = AdvanceSearcher()
            >>> api.get_data('21', '2018-01-01','2018-08-10')
            >>> api.driver.quit()

        Returns:
            list -- 数据字典列表
        """
        self.ensure_init()
        self.nav_tab(self.tab_id)
        if not level_like(level_or_name):
            level = self.name_to_level(level_or_name)
        else:
            level = level_or_name
        start, end = sanitize_dates(start, end)
        self.to_level(level)
        # 首先需要解析元数据
        meta = self.get_level_meta_data(level)
        field_maps = meta['field_maps']
        # 当前数据项目中文名称
        name = meta['api_name']
        self._before_query()
        data = self._read_data_by_period(level, start, end)
        return cleaned_data(data, field_maps, name)
