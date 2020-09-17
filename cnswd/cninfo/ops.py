import json
import random
import re
import time
from collections import namedtuple

from retry import retry
from retry.api import retry_call
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from toolz import groupby

from ..setting.config import TIMEOUT

DT_PAT = re.compile("date|year")
MENU = namedtuple('MENU', 'pos, name, code')


# region 菜单


def find_pos_list(self, root_css):
    """独立菜单位置列表【不含子级菜单】"""
    css = f"{root_css} li[data-pos]"
    lis = self.driver.find_elements_by_css_selector(css)
    pos_list = [li.get_attribute('data-pos') for li in lis]
    levels = groupby(len, pos_list)
    menu_pos = []
    len_max = max(levels.keys())
    menu_pos.extend(levels[len_max])
    for l in range(len_max-1, 0, -1):
        ps = set([x[:l] for x in levels[l+1]])
        to_add = set(levels[l]) - ps
        menu_pos.extend(to_add)
    return sorted(menu_pos)


def find_menu_name_by(self, pos, root_css):
    """根据菜单位置查找菜单名称"""
    css = f"{root_css} li[data-pos='{pos}']"
    li = self.driver.find_element_by_css_selector(css)
    # 可能菜单处于隐藏状态，执行脚本获取text
    js = "return arguments[0].innerText;"
    return self.driver.execute_script(js, li)


def find_menu(self, pos):
    """定位菜单元素

    Args:
        pos (str): 菜单位置字符串

    Returns:
        element: 菜单元素对象
    """
    li_css_fmt = 'li[data-pos="{}"]'
    for i in range(1, len(pos)+1):
        current_pos = pos[:i]
        css = li_css_fmt.format(current_pos)
        li = self.driver.find_element_by_css_selector(css)
        if 'active' not in li.get_attribute('class'):
            li.find_element_by_tag_name('a').click()
    return li


def parse_menu_info(li):
    """解析菜单信息

    Args:
        elem (element): 菜单元素【li】

    Returns:
        namedtuple: 菜单信息命名元组
    """
    a = li.find_element_by_tag_name('a')
    return MENU(
        pos=li.get_attribute('data-pos'),
        name=a.text,
        code=a.get_attribute('data-code')
    )


def get_all_menu_info(self, root_css):
    """获取网页菜单信息列表

    Args:
        root_css (str): 菜单根元素css样式

    Returns:
        list: 菜单信息命名元组列表
    """
    pos_list = find_pos_list(self, root_css)
    res = []
    for pos in pos_list:
        elem = find_menu(self, pos)
        res.append(parse_menu_info(elem))
    return res


def get_current_menu(self):
    """获取当前活动菜单对象"""
    css = f"{self.css.menu_root} li.active[data-pos]"
    lis = self.driver.find_elements_by_css_selector(css)
    li = max(lis, key=lambda li: li.get_attribute('data-pos'))
    return parse_menu_info(li)

# endregion

# region 元数据


def find_menu_request(self, menu):
    """查找菜单对应的请求"""
    # 有且仅有一条请求
    path = f'info?gatewayCode={menu.code}'
    for r in self.driver.requests:
        if r.method == 'GET' and path in r.url:
            return r


def parse_meta_data(self):
    """解析元数据"""
    meta = {}
    menu = get_current_menu(self)
    request = find_menu_request(self, menu)
    # 等待响应完成后才解析
    request = self.driver.wait_for_request(request.path)
    response = request.response
    if response.status_code == 200:
        # 直接使用 request.response.body
        # request.response 显示 body=b''
        # request.response.body则显示具体内容
        # 以下为调用后台方法
        # body = request._client.get_response_body(request.id)
        body = request.response.body
        json_data = json.loads(body)
        if json_data['msg'] == 'success':
            data = json_data['data']
            meta['api_level'] = menu.pos
            meta['api_name'] = data['baseInfo']['alias']
            meta['apiId'] = data['baseInfo']['name']
            meta['api_path'] = data['requestConfig']['requestPath']
            meta['inputParameter'] = data['requestConfig']['inputParameter']
            meta['field_maps'] = json.loads(
                data['resultContent']['outputParameter'])
    # 解析完成后，务必删除所有请求，否则会导致请求定位误差
    del self.driver.requests
    return meta


def get_condition_style(self, pos, suffix):
    """获取输入条件各元素显示风格属性"""
    self.to_level(pos)
    css = self.css.condition_fmt.format(suffix)
    elem = self.driver.find_element_by_css_selector(css)
    return elem.get_attribute('style')


def db_date_filter_mode(styles):
    """根据时间过滤元素显示风格提示输入类型"""
    A = 'display: none;'
    B = 'display: inline-block;'
    if all([x == A for x in styles]):
        return None
    if styles[0] == B and styles[1] == B:
        return 'YQ'
    if styles[3] == B:
        return 'YY'
    return 'DD'


def get_db_date_filter_mode(self, pos):
    """数据浏览器时间过滤类型"""
    n_list = [1, 2, 3, 4]
    styles = [get_condition_style(self, pos, n) for n in n_list]
    return db_date_filter_mode(styles)

# endregion


# region 选择股票代码

def toggler_market_open(self, data_id):
    li_css = self.css.market_css
    li = self.driver.find_element_by_css_selector(li_css)
    if 'opened' not in li.get_attribute('class'):
        li.find_element_by_tag_name('span').click()
    span_css = f"{li_css} a[data-id='{data_id}']"
    span = self.driver.find_element_by_css_selector(span_css)
    span.click()

# endregion

# region 输入时间过滤条件


def simulated_click(self, em):
    """使用脚本模拟点击"""
    self.driver.execute_script("arguments[0].click();", em)


def clear_date(self):
    """清除当前光标所在输入框的日期文本"""
    elem = self.driver.find_element_by_css_selector(self.css.clear_date)
    simulated_click(self, elem)


def datepicker(self, date_str, css, use_tab=True):
    """设定查询日期"""
    elem = self.driver.find_element_by_css_selector(css)
    elem.click()
    clear_date(self)
    if use_tab:
        elem.send_keys(date_str, Keys.TAB)
    else:
        elem.send_keys(date_str)
    # 合理等待响应
    self.driver.implicitly_wait(0.2)
    # self.driver.save_screenshot(f"{date_str}.png")


# def select_year(self, year, css=None):
#     js = "arguments[0].setAttribute('value', arguments[1]);"
#     # YQ -> '#se1_sele'
#     # YY -> '#se2_sele'
#     if css is None:
#         css = self.css.select_year
#     elem = self.driver.find_element_by_css_selector(css)
#     self.driver.execute_script(js, elem, str(year))


def select_year(self, year, css=None):
    # YQ -> '#se1_sele'
    # YY -> '#se2_sele'
    if css is None:
        css = self.css.select_year
    elem = self.driver.find_element_by_css_selector(css)
    elem.clear()
    elem.send_keys(str(year))


def select_quarter(self, q):
    elem = self.driver.find_element_by_css_selector(self.css.select_quarter)
    t2 = int(q)
    assert t2 in (1, 2, 3, 4), '季度有效值为(1,2,3,4)'
    select = Select(elem)
    select.select_by_index(t2 - 1)


class element_text_change_to(object):
    """期望元素文本改变为指定文本
    """

    def __init__(self, locator, text):
        """
        Arguments:
            locator {定位元组} -- 定位对象  
            text {str} -- 元素文本内容
        """
        self.locator = locator
        self.text = text

    def __call__(self, driver):
        # Finding the referenced element
        element = driver.find_element(*self.locator)
        return self.text == element.text


class element_attribute_change_to(object):
    """期望元素属性更改为指定值
    """

    def __init__(self, locator, name, attribute):
        """
        Arguments:
            locator {定位元组} -- 定位对象  
            name {str} -- 元素属性名称
            attribute {str} -- 属性值
        """
        self.locator = locator
        self.name = name
        self.attribute = attribute

    def __call__(self, driver):
        element = driver.find_element(*self.locator)
        # 当指定名称的属性变更为指定属性时，返回该元素
        if self.attribute == element.get_attribute(self.name):
            return element
        else:
            return False


def input_code(self, code):
    """输入查询代码

    Args:
        driver (driver): 浏览器
        code (str): 完整股票代码
        id_name (str, optional): 元素id. Defaults to 'input_code'.
    """
    elem = self.driver.find_element_by_css_selector(self.css.input_code)
    elem.clear()
    elem.send_keys(code)
    locator = (By.CSS_SELECTOR, self.css.search_code)
    searched = self.wait.until(EC.element_to_be_clickable(locator))
    result = searched.find_elements_by_tag_name('p')
    if len(result) != 1:
        raise ValueError(f"无效股票代码{code}")
    result[0].click()

# endregion

# region 解析输出结果


def parse_response(self, request):
    """解析指定请求的响应数据

    Returns:
        list: 数据字典列表
    """
    response = request.response
    res = []  # 默认为空
    if response is None:
        return res
    if response.status_code == 200:
        # 同上说明
        # body = request._client.get_response_body(request.id)
        body = request.response.body
        data = json.loads(body)
        # 解析结果 402 不合法的参数
        if data['resultcode'] == 200:
            # 记录列表
            res = data['records']
    return res


def is_target_request(self, request, api_path):
    """判断网址是否为目标请求"""
    filter_pattern = self._filter_pattern
    if not filter_pattern:
        return api_path in request.url
    # 网址模式匹配
    # pattern = '(.*?)?'.join([f"{k}={v}" for k, v in filter_pattern.items()])
    # pattern = re.compile(f"{api_path}?(.*?)?{pattern}")
    # return re.match(pattern, url) is not None
    d = request.params
    if api_path in request.url:
        try:
            cond = all([d[k] == v for k, v in filter_pattern.items()])
            return cond
        except KeyError:
            return False
    else:
        return False


def find_data_requests(self, level):
    """查找数据目标请求"""
    # 使用url作为键，避免重复采集发出的请求
    requests = {}
    api_path = self.get_level_meta_data(level)['api_path']
    for r in self.driver.requests:
        if r.method == 'POST' and is_target_request(self, r, api_path):
            requests[r.url] = r
    return requests.values()


def read_json_data(self, level):
    """解析查询命令返回的数据"""
    data = []
    requests = find_data_requests(self, level)
    for request in requests:
        # 在读取数据前必须确定完成加载
        # request = self.driver.wait_for_request(r.path)
        docs = parse_response(self, request)
        data.extend(docs)
    # 删除已经读取的请求
    del self.driver.requests
    return data

# endregion
