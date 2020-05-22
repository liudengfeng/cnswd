"""

深证信基础模块

"""
import math
import os
import re
import time

import pandas as pd
from bs4 import BeautifulSoup
from selenium.common.exceptions import (ElementNotInteractableException,
                                        NoSuchElementException,
                                        TimeoutException)
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait

from .._exceptions import MaybeChanged, RetryException
from .._selenium import make_headless_browser
from ..setting.config import POLL_FREQUENCY, TIMEOUT
from ..utils.log_utils import make_logger
from ..utils.pd_utils import _concat
from .ops import (element_attribute_change_to, navigate, read_html_table,
                  wait_page_loaded)

HOME_URL_FMT = 'http://webapi.cninfo.com.cn/#/{}'
PAGINATION_PAT = re.compile(r'共\s(\d{1,})\s条记录')


def remove_duplicates(x):
    """移除列名称中的重复项"""
    if isinstance(x, int):
        return x
    n = len(x)
    if x[:int(n / 2)] == x[-int(n / 2):]:
        return x[:int(n / 2)]
    else:
        return x


def normalize_code(df):
    if '证券代码' in df.columns:
        df.证券代码 = df.证券代码.map(lambda x: str(x).zfill(6))
    if '股票代码' in df.columns:
        df.股票代码 = df.股票代码.map(lambda x: str(x).zfill(6))
    if '指数代码' in df.columns:
        df.指数代码 = df.指数代码.map(lambda x: str(x).zfill(6))
    return df


class SZXPage(object):
    """深证信基础网页"""

    # 变量
    code_loaded = False
    current_t1_value = ''  # 开始日期
    current_t2_value = ''  # 结束日期
    current_level = ''  # 顶部菜单层级

    # 子类需要改写的属性
    api_name = ''
    api_e_name = ''
    config = {}
    # 以此元素是否显示为标准，检查页面是否正确加载
    check_loaded_css = ''
    check_loaded_css_value = ''

    data_loaded_css = ''
    data_loaded_css_value = ''
    attrs = {'id': ''}  # 数据表id

    level_input_css = ''
    level_query_bnt_css = ''
    preview_btn_css = ''  # 预览数据按钮
    wait_for_preview_css = ''  # 检验预览结果css
    view_selection = {}  # 可调显示行数 如 {1:10,2:20,3:50}

    def __init__(self, log_to_file=None):
        start = time.time()
        self.log_to_file = log_to_file
        self.driver = make_headless_browser()
        name = f"{self.api_name}{str(os.getpid()).zfill(6)}"
        self.logger = make_logger(name, log_to_file)
        self.wait = WebDriverWait(self.driver, TIMEOUT, POLL_FREQUENCY)
        self._load_page()
        self.driver.maximize_window()
        # 确保加载完成
        self.driver.implicitly_wait(0.2)
        self.logger.notice(f'加载主页用时：{(time.time() - start):>0.4f}秒')

    def _load_page(self):
        # 如果重复加载同一网址，耗时约为1ms
        self.logger.info(self.api_name)
        url = HOME_URL_FMT.format(self.api_e_name)
        self.driver.get(url)
        msg = f"首次加载{self.api_name}超时"
        # 特定元素可见，完成首次页面加载
        wait_page_loaded(self.wait, self.check_loaded_css,
                         self.check_loaded_css_value, msg)

    def _ul_num_map(self):
        """二级导航所对应的ul元素序号映射"""
        res = {}
        for level in self.config.keys():
            ls = level.split('.')
            level_1 = ls[0]
            if len(ls) >= 2 and res.get(level_1) is None:
                res[level_1] = len(res) + 1
        return res

    def _select_level(self, level):
        msg = f'"{self.api_name}"指标导航可接受范围：{list(self.config.keys())}'
        assert level in self.config.keys(), msg
        navigate(self.driver, level)

    @property
    def current_t1_css(self):
        return self.config[self.current_level]['css'][0]

    @property
    def current_t2_css(self):
        return self.config[self.current_level]['css'][1]

    def reset(self):
        self.scroll(0.0)
        # self.driver.quit()
        self.driver.refresh()

        # 恢复变量默认值
        self.code_loaded = False
        self.current_level = ''
        self.current_t1_value = ''  # 开始日期
        self.current_t2_value = ''  # 结束日期

        start = time.time()
        # self.driver = make_headless_browser()
        name = str(os.getpid()).zfill(6)
        self.logger = make_logger(name, self.log_to_file)
        # self.wait = WebDriverWait(self.driver, TIMEOUT)
        self._load_page()
        self.driver.maximize_window()
        self.logger.notice(f'重新加载主页用时：{(time.time() - start):>0.4f}秒')

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.driver.quit()

    def __repr__(self):
        msg = self._view_message('', self.current_level, self.current_t1_value,
                                 self.current_t2_value)
        return msg

    def _wait_for_activate(self, data_name, status='active'):
        """等待元素激活"""
        xpath_fmt = "//a[@data-name='{}']"
        locator = (By.XPATH, xpath_fmt.format(data_name))
        self.wait.until(element_attribute_change_to(locator, 'class', status),
                        f'{self.api_name} {data_name} 激活元素超时')

    def _wait_for_visibility(self, elem_css, msg=''):
        """
        等待指定css元素可见

        Arguments:
            elem_css {str} -- 可见元素的css表达式
        """
        m = EC.visibility_of_element_located((By.CSS_SELECTOR, elem_css))
        self.wait.until(m, message=msg)

    def _wait_for_all_presence(self, elem_css, msg=''):
        """
        等待指定css的所有元素出现

        Arguments:
            elem_css {str} -- 元素的css表达式
        """
        m = EC.presence_of_all_elements_located((By.CSS_SELECTOR, elem_css))
        self.wait.until(m, message=msg)

    def _get_count_tip(self, span_css):
        """获取元素数量提示"""
        i = self.driver.find_element_by_css_selector(
            span_css).find_element_by_tag_name('i')
        try:
            return int(i.text)
        except:
            return 0

    def click_elem(self, elem):
        """点击所选元素"""
        self.driver.execute_script("arguments[0].scrollIntoView();", elem)
        actions = ActionChains(self.driver)
        actions.move_to_element(elem).click().perform()
        # name = f"{elem.text}.png"
        # self.driver.save_screenshot(name)

    def _add_or_delete_all(self, label_css, btn_css):
        """添加或删除所选全部元素"""
        # 点击全选元素
        self.driver.find_element_by_css_selector(label_css).click()
        # 点击命令按钮
        self.driver.find_element_by_css_selector(btn_css).click()

    def _query(self, input_text, input_css, query_bnt_css):
        """
        执行查询

        在指定`input_css`元素输入`input_text`，点击`query_bnt_css`
        执行查询
        """
        input_elem = self.driver.find_element_by_css_selector(input_css)
        input_elem.clear()
        input_elem.send_keys(input_text)
        self.driver.find_element_by_css_selector(query_bnt_css).click()

    def _no_data(self):
        """等待预览呈现后，首先需要检查查询是否无数据返回"""
        try:
            e = self.driver.find_element_by_css_selector('.center')
            if e.text in ('暂无记录', ):
                return True
            return False
        except Exception:
            return False

    def _has_exception(self):
        """等待预览呈现后，尽管有数据返回，检查是否存在异常提示"""
        csss = ['.tips', '.cancel', '.timeout', '.sysbusy']
        for css in csss:
            try:
                elem = self.driver.find_element_by_css_selector(css)
                if elem.get_attribute('style') == 'display: inline;':
                    msg = self + '\n', elem.text
                    self.logger.notice(msg)
                    return True
            except Exception:
                return False

    def _get_row_num(self):
        """获取预览输出的总行数"""
        pagination_css = '.pagination-info'
        pagination = self.driver.find_element_by_css_selector(pagination_css)
        row_num = int(re.search(PAGINATION_PAT, pagination.text).group(1))
        return row_num

    def _get_pages(self):
        """获取预览输出的总页数"""
        # 如无法定位到`.page-last`元素，可能的情形
        # 1. 存在指示页数的li元素，倒数第2项的li元素text属性标示页数；
        # 2. 不存在li元素，意味着只有1页
        try:
            li_css = '.page-last'
            li = self.driver.find_element_by_css_selector(li_css)
            return int(li.text)
        except Exception:
            pass
        # 尝试寻找li元素
        try:
            li_css = 'ul.pagination li'
            lis = self.driver.find_elements_by_css_selector(li_css)
            return int(lis[-2].text)
        except Exception:
            return 1

    def _auto_change_view_row_num(self):
        """自动调整到每页最大可显示行数"""
        min_row_num = min(self.view_selection.values())
        max_row_num = max(self.view_selection.values())
        total = self._get_row_num()

        if total <= min_row_num:
            nth = min(self.view_selection.keys())
        elif total >= max_row_num:
            nth = max(self.view_selection.keys())
        else:
            for k, v in self.view_selection.items():
                if total <= v:
                    nth = k
                    break
        # 只有总行数大于最小行数，才有必要调整显示行数
        if total > min_row_num:
            # 点击触发可选项
            self.driver.find_element_by_css_selector(
                '.dropdown-toggle').click()
            locator = (By.CSS_SELECTOR, '.btn-group')
            try:
                self.wait.until(
                    element_attribute_change_to(locator, 'class',
                                                'btn-group dropup open'),
                    '调整每页显示行数超时')
            except TimeoutException:
                self.driver.implicitly_wait(0.5)
            css = '.btn-group > ul:nth-child(2) li'
            lis = self.driver.find_elements_by_css_selector(css)
            lis[nth - 1].click()

    def _before_read(self, bt):
        raise NotImplementedError('子类必须完成读取前的准备')

    def _read_html_data(self, bt=True):
        """通用读取网页数据"""
        self._before_read(bt)
        if self._no_data():
            return pd.DataFrame()
        # 是否存在异常
        if self._has_exception():
            item = self.config[self.current_level]['name']
            raise RetryException(f'项目：{item} 提取的网页数据不完整')
        # 当表头不显示时，数据以序列形式读取；
        # 存在
        table_id = self.attrs['id']
        table = self.driver.find_element_by_id(table_id)
        thead = table.find_element_by_tag_name('thead')
        thead_style = thead.get_attribute('style')
        if thead_style == '':
            return self._read_html_table()
        elif thead_style == 'display: none;':
            return self._read_series()
        else:
            raise MaybeChanged('数据表头显示风格属性可能已经调整！')

    def _read_series(self):
        """读取以序列数据"""
        soup = BeautifulSoup(self.driver.page_source, "lxml")
        # √ 使用
        table_id = f"#{self.attrs['id']}"
        table = soup.select_one(table_id)
        titles = table.select('span[class="title"]')
        values = table.select('span[class="value"]')
        assert len(titles) == len(values), '标题与值数量应相等'
        res = {}
        for t, v in zip(titles, values):
            # √ 以下方式才能避免无限递归错误
            res[t['title']] = [v['title']]
        # 序列数据以DataFrame返回
        df = pd.DataFrame.from_dict(res)
        return normalize_code(df)

    def _read_html_table(self):
        """读取当前网页数据表"""
        # TODO:以下部分在子类中`_before_read`完成
        # # 点击`预览数据`
        # if self.api_e_name == 'thematicStatistics':
        #     # 专题统计中，部分项目无命令按钮
        #     if any(self.config[self.current_level]['css']):
        #         # 预览数据
        #         self.driver.find_element_by_css_selector(
        #             self.preview_btn_css).click()
        #     else:
        #         # 没有预览按钮时，等待一小段时间
        #         self.driver.implicitly_wait(0.1)
        # elif self.api_e_name == 'marketData':
        #     # 预览数据
        #     self.driver.find_element_by_css_selector(
        #         self.preview_btn_css).click()
        # else:
        #     # 预览数据
        #     self.driver.find_element_by_css_selector(
        #         self.preview_btn_css).click()
        # 数据加载与tip好像有延时
        # self.driver.implicitly_wait(0.1)
        # if self.api_e_name not in ('marketData', 'marketZhishu'):
        #     # 等待预览数据完成加载。如数据量大，可能会比较耗时。最长约6秒。
        #     self._wait_for_preview(style='display: none;')

        # 是否无数据返回
        # 测试表明，专题统计不一定能准确捕获`.no-records-found`提示，导致后续无法获取数据行数
        # 多次运行，可解决此类问题

        # 自动调整显示行数，才读取页数
        self._auto_change_view_row_num()
        pages = self._get_pages()
        n_width = 5  # 行数不超过5位数
        dfs = []
        na_values = ['-', '无', ';']
        if pages == 1:
            df = pd.read_html(self.driver.page_source,
                              na_values=na_values,
                              attrs=self.attrs)[0]
            dfs.append(df)
            self.logger.info(f'>> 分页 第{1:{n_width}}页 / 共{pages:{n_width}}页')
        else:
            for i in range(1, pages + 1):
                df = read_html_table(self.driver, i, self.attrs)
                self.logger.info(
                    f'>> 分页 第{i:{n_width}}页 / 共{pages:{n_width}}页')
                dfs.append(df)
        res = _concat(dfs)
        # 去除可能重复的表头
        res.columns = res.columns.map(remove_duplicates)
        res = normalize_code(res)
        return res

    def _change_year(self, css, year):
        """改变查询指定id元素的年份"""
        elem = self.driver.find_element_by_css_selector(css)
        elem.clear()
        elem.send_keys(str(year))

    def _datepicker(self, css, date_str):
        """指定日期"""
        elem = self.driver.find_element_by_css_selector(css)
        elem.clear()
        elem.send_keys(date_str, Keys.TAB)

    def _view_message(self, p, level, start, end, s=''):
        """构造显示信息"""
        width = 30
        if level is None:
            item = ''
        else:
            item = self.config[level]['name']
        if pd.api.types.is_number(start):
            if pd.api.types.is_number(end):
                msg = f'{start}年{end}季度'
            else:
                if end:
                    msg = f'{start}年 ~ {end}'
                else:
                    msg = f'{start}年'
        else:
            if start:
                msg = pd.Timestamp(start).strftime(r'%Y-%m-%d')
                if end:
                    msg += f" ~ {pd.Timestamp(end).strftime(r'%Y-%m-%d')}"
                else:
                    msg = f"{pd.Timestamp(start).strftime(r'%Y-%m-%d')} ~ {pd.Timestamp('today').strftime(r'%Y-%m-%d')}"
            else:
                msg = ''
        left = f"{p}{item}"
        return f"{left:{width}} {msg} {s}"

    def _log_info(self, p, level, start, end, s=''):
        self.logger.info(self._view_message(p, level, start, end, s))

    def scroll(self, size):
        """
        上下滚动到指定位置

        参数:
        ----
        size: float, 屏幕自上而下的比率
        """
        # 留存
        # 滚动到屏幕底部
        # self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        h = self.driver.get_window_size()['height']
        js = f"var q=document.documentElement.scrollTop={int(size * h)}"
        self.driver.execute_script(js)
