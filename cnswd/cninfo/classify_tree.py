import json
import os
import re
import time

import pandas as pd
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support.ui import Select, WebDriverWait

from .._seleniumwire import make_headless_browser
from ..setting.config import POLL_FREQUENCY, TIMEOUT
from ..utils.log_utils import make_logger
from ..utils.pd_utils import _concat
from .ops import toggler_close, toggler_open, wait_page_loaded

HOME_URL_FMT = 'http://webapi.cninfo.com.cn/#/{}'
CLASS_ID = re.compile(r'platetype=(.*?)(&|$)')

PLATE_LEVELS = {
    1: '137004',
    2: '137002',
    3: '137003',
    4: '137006',
    5: '137007',
    6: '137001',
}
PLATE_MAPS = {
    '137001': '市场分类',
    '137002': '证监会行业分类',
    '137003': '国证行业分类',
    '137004': '申万行业分类',
    '137006': '地区分类',
    '137007': '指数分类',
}


class _LevelEncoder(object):
    """自然顺序层级编码器

    Notes：
    ------
        编码按照输入顺序，而非代码本身的顺序。
    """

    def __init__(self):
        self.infoes = {}
        self.level = {}

    def _on_changed(self, i, depth):
        # 子级别重置为1，当前级别+1，父级别不变
        for loc in range(i + 1, depth):
            self.level[loc] = 1
        old = self.level.get(i, 1)
        self.level[i] = old + 1

    def encode(self, code_tuple):
        """输入代码元组，输出自然顺序层级编码

        Arguments:
            code_tuple {tuple}} -- 编码元组。如(1,'S11','01','03')

        Returns:
            str -- 以`.`分隔的层级。如`2.3.4.1`
        """
        depth = len(code_tuple)
        for i in range(depth):
            current = code_tuple[i]
            if self.infoes.get(i, None) is None:
                self.infoes[i] = current
                # 初始赋值 nth.1...
                self.level[0] = code_tuple[0]
                for i in range(1, depth):
                    self.level[i] = 1
            else:
                if self.infoes[i] != current:
                    # 当出现不一致时，子级只记录，不再比较
                    for j in range(i, depth):
                        self.infoes[j] = code_tuple[j]
                    self._on_changed(i, depth)
                    break
        return '.'.join([str(x) for x in self.level.values()])


def _level_split(nth, code):
    """分解分类编码"""
    if nth == 1:
        return (nth, code[:3], code[3:5], code[5:7])
    elif nth == 2:
        if len(code) == 3:
            return (nth, code[0], code[1:])
        else:
            return (nth, code[:3], code[3:5], code[5:7])
    elif nth == 3:
        return (nth, code[:3], code[3:5], code[5:7], code[7:])
    elif nth == 4:
        return (nth, code[:2], code[2:])
    elif nth == 5:
        return (nth, code)
    elif nth == 6:
        return (nth, code)


class ClassifyTree(object):
    """分类树"""
    btned = False
    api_name = '分类树'
    api_e_name = 'dataBrowse'

    check_loaded_css = '.nav-second > div:nth-child(1) > h1:nth-child(1)'
    check_loaded_css_value = '数据浏览器'

    def __init__(self, log_to_file=None):
        self.log_to_file = log_to_file
        self.driver = None

    def _ensure_init(self):
        url = 'http://webapi.cninfo.com.cn/#/dataBrowse'
        start = time.time()
        self.driver = make_headless_browser()
        self.wait = WebDriverWait(self.driver, TIMEOUT, POLL_FREQUENCY)
        name = f"{self.api_name}{str(os.getpid()).zfill(6)}"
        self.logger = make_logger(name, self.log_to_file)
        self.driver.get(url)
        # 确保加载完成
        msg = f"首次加载{self.api_name}超时"
        # 特定元素可见，完成首次页面加载
        wait_page_loaded(self.wait, self.check_loaded_css,
                         self.check_loaded_css_value, msg)
        self.driver.implicitly_wait(0.5)
        self.logger.notice(f'加载主页用时：{(time.time() - start):>0.4f}秒')

    def reset(self):
        """恢复至初始状态"""
        self._ensure_init()
        # 一般而言出现异常均有提示信息出现
        try:
            self.driver.switch_to.alert.dismiss()
        except Exception:
            pass
        # 刷新浏览器
        self.driver.refresh()
        # self.driver.implicitly_wait(0.5)
        time.sleep(1.5)
        self.btn2()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if self.driver is not None:
            self.driver.quit()

    def btn2(self):
        """高级搜索"""
        css = '#btn2'
        self.driver.find_element_by_css_selector(css).click()

    def _construct_css(self, level):
        nums = level.split('.')
        head = f'.classify-tree > li:nth-child({nums[0]})'
        if len(nums) == 1:
            return head
        rest = [
            'ul:nth-child(2) > li:nth-child({})'.format(x) for x in nums[1:]
        ]
        return ' > '.join([head] + rest)

    def _construct_path(self, a):
        """构造api地址"""
        api = a.get_attribute('data-api')
        param = a.get_attribute('data-param')
        return f"{api}?{param}"

    # @retry(stop=stop_after_attempt(5), wait=wait_fixed(0.3))
    def _get_classify_tree(self, level, values):
        """获取层股票列表"""
        df = pd.DataFrame()
        cum_level = []
        for l in level.split('.'):
            cum_level.append(l)
            css = self._construct_css('.'.join(cum_level))
            li = self.driver.find_element_by_css_selector(css)
            toggler_open(li)
            # self.driver.save_screenshot(f"{'.'.join(cum_level)}.png")
            if li.get_attribute('class') == 'tree-empty':
                # 点击加载分类项下股票代码
                a = li.find_element_by_tag_name('a')
                a.click()
                path = self._construct_path(a)
                # Wait for the request/response to complete
                request = self.driver.wait_for_request(path)
                response = request.response

                data = json.loads(response.body)
                num = data['total']
                self.logger.info(f'分类树层级：{level} 行数:{num}')
                if num >= 1:
                    records = data['records']
                    df['股票代码'] = [x['SECCODE'] for x in records]
                    df['股票简称'] = [x['SECNAME'] for x in records]

                    df['分类层级'] = level
                    df['分类编码'] = values[0]
                    df['分类名称'] = values[1]
                    df['平台类别'] = values[2]
        # # 关闭根树
        css = self._construct_css(level.split('.')[0])
        li = self.driver.find_element_by_css_selector(css)
        toggler_close(li)
        return df

    @property
    def classify_bom(self):
        """股票分类BOM表"""
        if self.driver is None:
            self._ensure_init()
        self.btn2()
        roots = self.driver.find_elements_by_css_selector(
            '.classify-tree > li')
        items = []
        for r in roots:
            # 需要全部级别的分类编码名称
            items.extend(r.find_elements_by_tag_name('span'))
            items.extend(r.find_elements_by_tag_name('a'))
        data = []
        attrs = ('data-id', 'data-name')
        for item in items:
            data.append([item.get_attribute(name) for name in attrs])
        df = pd.DataFrame.from_records(data, columns=['分类编码', '分类名称'])
        return df.dropna().drop_duplicates(['分类编码', '分类名称'])

    def get_tree_attribute(self, nth):
        """获取分类树属性"""
        if self.driver is None:
            self._ensure_init()
        self.btn2()
        res = {}
        encoder = _LevelEncoder()
        valid_plate = PLATE_LEVELS[nth]
        tree_css = '.classify-tree > li:nth-child({})'.format(nth)
        li = self.driver.find_element_by_css_selector(tree_css)
        trees = li.find_elements_by_xpath(
            './/li[@class="tree-empty"]//descendant::a')
        for tree in trees:
            name = tree.get_attribute('data-name')
            id_ = tree.get_attribute('data-id')
            param = tree.get_attribute('data-param')
            plate = re.search(CLASS_ID, param).group(1)
            code_tuple = _level_split(nth, id_)
            level = encoder.encode(code_tuple)
            if plate == valid_plate:
                res[level] = (id_, name, plate)
        # # 注意:证监会 综合分类混乱。编码 2.19 开头
        return res

    def get_classify_tree(self, n):
        """获取分类树层级下的股票列表"""
        if self.driver is None:
            self._ensure_init()
        self.btn2()
        self.driver.implicitly_wait(0.2)
        levels = self.get_tree_attribute(n)
        status = {}
        res = []
        for level, values in levels.items():
            for i in range(1, 5):
                if status.get(level, False):
                    break
                try:
                    df = self._get_classify_tree(level, values)
                    res.append(df)
                    status[level] = True
                except NoSuchElementException:
                    # 无此元素代表该层级无数据，跳出循环
                    status[level] = True
                    break
                except Exception as e:
                    self.logger.info(f"第{i}次尝试")
                    self.logger.error(e)
                    self.reset()
                    status[level] = False
        for k in status.keys():
            if not status[k]:
                print(f'层级{k}失败')
        df = _concat(res)
        return df
