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
from .ops import (datepicker, element_attribute_change_to,
                  element_text_change_to, find_by_id, input_code,
                  parse_response, select_quarter, select_year,
                  simulated_click, find_request)
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
    query_btn_css = '.stock-search'
    delay = 0
  
    def nav_tab(self, nth):
        """选择快速搜索或高级搜索

        Args:
            nth (int): 1代表快速搜索，2代表高级搜索
        """
        current_tab = getattr(self, 'current_tab', None)
        if current_tab == nth:
            return
        assert nth in (1, 2)
        css = "btn{}".format(nth)
        btn = self.wait.until(EC.element_to_be_clickable((By.ID, css)))
        btn.click()
        self.current_tab = nth

    # 基于日期过滤元素显示特性判断输入方式
    def _current_dt_filter_mode(self):
        """当前时间过滤输入模式"""
        cond_css = [
            '.condition1', '.condition2', 'div.filter-condition:nth-child(3)',
            '.condition4'
        ]
        elems = [
            self.driver.find_element_by_css_selector(css) for css in cond_css
        ]
        res = [self._is_view(elem) for elem in elems]
        if not any(res):
            return None  # 无时间限制
        elif res[3] and sum(res[:3]) == 0:
            return 'Y'  # 只输入年
        elif all(res[:2]) and sum(res[2:]) == 0:
            return 'YQ'  # 输入年、季度
        elif res[2] and sum(res) == 1:
            return 'DD'  # 输入 date ~ date
        raise ValueError(f"算法出错")

    def _current_period_type(self):
        """期间内部循环类型"""
        # 根据输入参数有关日期部分决定日期设置行为
        # 第一个字符代表freq
        # freq D | M | Q | Y
        # 第二个字符代表日期格式
        # 表达式 Y: 2019 | Q 2019 1 | D 2019-01-11
        mode = self._current_dt_filter_mode()
        tab_id = self.tab_id
        level = self.api_level
        if mode is None:
            return None
        elif mode == 'DD':
            # 网站设定限制，每次返回最大20000行
            # 数量量大的项目，每次取一个月的数据
            # 由于可能存在网站菜单栏目变动问题，映射表可能发生变化
            # 当前硬编码将`投资评级`设定为按月循环，数据量大约在10000行以内
            # 至于其他栏目甚至可以一次性读取，此处简化为统一按季度内部循环
            if tab_id == 2 and level in ('3', ):
                return 'MD'
            # 否则按季度循环
            return 'QD'
        elif mode == 'YQ':
            # 按季度循环，输入年、季度
            return 'QQ'
        elif mode == 'Y':
            # 按年循环，只输入年份
            return 'YY'
        raise ValueError("期间类型可能设置错误")

    def _set_date_filter(self, t1, t2):
        mode = self._current_period_type()
        if mode is None:
            return
        dt_fmt = mode[1]
        # 开始日期 ~ 结束日期
        if dt_fmt == 'D':
            if t1:
                css_1 = "input.date:nth-child(1)"
                datepicker(self, t1, css_1)
            if t2:
                css_2 = "input.form-control:nth-child(2)"
                datepicker(self, t2, css_2)
        # 年 季度
        if t1 and dt_fmt == 'Q':
            select_year(self, t1)
            select_quarter(self, t2)
        # 年
        if t1 and dt_fmt == 'Y':
            # 特殊
            select_year(self, t1, 'se2')

    def _before_read(self):
        """等待数据完成加载"""
        css = '.span_target'
        locator = (By.CSS_SELECTOR, css)
        self.wait.until(
            element_attribute_change_to(locator, 'style', 'display: none;'))
        page_css = '.pagination-info'
        page_elem = self.driver.find_element_by_css_selector(page_css)
        if page_elem.is_displayed():
            text = page_elem.text
            page = int(PAGE_PAT.findall(text)[0])
            self.logger.info(f"网页数据{page:5} 条记录")


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
        data = self._read_data_by_period(start, end)
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
        # 用时约3秒
        codes = getattr(self, '_codes', {})
        # 由于加载成功后会缓存在本地，后续加载会很快完成
        # 作为一个副产品，可以用于判断市场股票数量的依据
        nums = getattr(self, '_nums', {})
        if not nums:
            data_ids = ['101', '102', '107', '108', '110', '106', '109']
            for data_id in data_ids:
                api_elem = find_by_id(self, data_id)
                # 直接使用模拟点击，简化操作
                simulated_click(self, api_elem)
                self.driver.implicitly_wait(INTERVAL)
                data_name = api_elem.get_attribute('data-name')
                data_api = api_elem.get_attribute('data-api')
                data_param = api_elem.get_attribute('data-param')
                data_path = f"{data_api}?{data_param}"
                request = find_request(self, data_path)
                request = self.driver.wait_for_request(request.path)
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
        add_label_css = '.cont-top-right > div:nth-child(1) > div:nth-child(1) > label:nth-child(1) > i:nth-child(2)'
        add_btn_css = '.cont-top-right > div:nth-child(2) > div:nth-child(1) > button:nth-child(1)'
        check_css = '.cont-top-right > div:nth-child(1) > div:nth-child(1) > span:nth-child(2) > i:nth-child(1)'
        locator = (By.CSS_SELECTOR, check_css)
        data_ids = ['101', '102', '107', '108', '110', '106', '109']
        for data_id in data_ids:
            api_elem = find_by_id(self, data_id)
            # data_name = api_elem.get_attribute('data-name')
            # data_api = api_elem.get_attribute('data-api')
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
                self._add_or_delete_all(add_label_css, add_btn_css)
        total_css = '.cont-top-right > div:nth-child(3) > div:nth-child(1) > span:nth-child(2) > i:nth-child(1)'
        actual = int(self.driver.find_element_by_css_selector(total_css).text)
        expected = sum(self._nums.values())
        assert actual == expected, f"股票总数应为：{expected}，实际：{actual}"
        self._code_loaded = True

    def _select_all_fields(self):
        """全选字段"""
        # 使用i元素
        # label_css = '.detail-cont-bottom > div:nth-child(1) > div:nth-child(1) > label:nth-child(1)'
        label_css = '.detail-cont-bottom > div:nth-child(1) > div:nth-child(1) > label:nth-child(1) > i:nth-child(2)'
        btn_css = '.detail-cont-bottom > div:nth-child(2) > div:nth-child(1) > button:nth-child(1)'
        # 全选数据字段
        self._add_or_delete_all(label_css, btn_css)

    def _add_or_delete_all(self, label_css, btn_css):
        """添加或删除所选全部元素"""
        # 点击全选元素
        self.driver.find_element_by_css_selector(label_css).click()
        # 点击命令按钮
        self.driver.find_element_by_css_selector(btn_css).click()

    def _before_query(self):
        """执行查询前应完成的动作"""
        # 高级搜索需要加载全部代码、全选擦查询字段
        self._load_all_code()
        check_css = '.detail-cont-bottom > div:nth-child(3) > div:nth-child(1) > span:nth-child(2) > i:nth-child(1)'
        num = int(self.driver.find_element_by_css_selector(check_css).text)
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
        self.current_item = name = meta['api_name']
        self._before_query()
        data = self._read_data_by_period(start, end)
        return cleaned_data(data, field_maps, name)
