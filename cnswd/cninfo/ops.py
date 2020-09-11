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

from ..setting.config import TIMEOUT


DT_PAT = re.compile("date|year")
MENU = namedtuple('MENU', 'pos, name, code')


def find_request(self, path):
    for r in self.driver.requests:
        if r.method == 'POST' and path in r.url:
            return r


def find_menu(self, pos):
    """定位菜单元素

    Args:
        pos (str): 菜单位置字符串

    Returns:
        element: 菜单元素对象
    """
    for i in range(1, len(pos)+1):
        css = f'li[data-pos="{pos[:i]}"]'
        elem = self.driver.find_element_by_css_selector(css)
        if 'active' not in elem.get_attribute('class'):
            elem.click()
    return elem


def parse_menu_info(elem):
    """解析菜单信息

    Args:
        elem (element): 菜单元素【li】

    Returns:
        namedtuple: 菜单信息命名元组
    """
    a = elem.find_element_by_tag_name('a')
    return MENU(
        pos=elem.get_attribute('data-pos'),
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
    menu_root = self.driver.find_element_by_css_selector(root_css)
    items = menu_root.find_elements_by_css_selector('li[data-pos]')
    pos_list = [e.get_attribute('data-pos') for e in items]
    res = []
    for pos in pos_list:
        elem = find_menu(self, pos)
        time.sleep(0.3)
        if 'has-child' not in elem.get_attribute('class'):
            res.append(parse_menu_info(elem))
    return res


def find_info_request(self, menu):
    path = f'info?gatewayCode={menu.code}'
    for r in self.driver.requests:
        if r.method == 'GET' and path in r.url:
            return r


@retry(AttributeError, tries=3, delay=1)
def parse_meta_data(self, menu):
    """解析元数据"""
    meta = {}
    request = find_info_request(self, menu)
    response = request.response
    if response.status_code == 200:
        body = request._client.get_response_body(request.id)
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
    # 删除当前请求
    del request
    return meta


def simulated_click(self, em):
    """使用脚本模拟点击"""
    self.driver.execute_script("arguments[0].click();", em)


def current_dt_filter_mode(level_meta):
    """根据输入参数有关日期字段名称判断循环模式"""
    api_level = level_meta['api_level']
    para_list = level_meta['inputParameter']
    f_names = [d['fieldName']
               for d in para_list if DT_PAT.findall(d['fieldName'])]
    if len(f_names) == 0:
        return None
    elif 'syear' in f_names:
        return 'YY'
    elif 'rdate' in f_names:
        return 'QQ'
    # 其实此算法并不一定正确，只是目前找不到相应规律
    # 放在最后确保最大程度减少失误
    # 数据浏览器凡是三层菜单，都是年季输入
    if len(api_level) == 3:
        return 'QQ'
    else:
        return 'DD'


def clear_date(self):
    """清除当前光标所在输入框的日期文本"""
    elem = self.driver.find_element_by_css_selector(self.css.clear_date)
    simulated_click(self, elem)


def datepicker(self, date_str, css):
    """设定查询日期"""
    elem = self.driver.find_element_by_css_selector(css)
    elem.click()
    clear_date(self)
    elem.send_keys(date_str, Keys.TAB)
    # 合理等待响应
    self.driver.implicitly_wait(0.2)


def select_year(self, year):
    js = "arguments[0].setAttribute('value', arguments[1]);"
    elem = self.driver.find_element_by_css_selector(self.css.select_year)
    self.driver.execute_script(js, elem, year)


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
        body = request._client.get_response_body(request.id)
        data = json.loads(body)
        # 解析结果 402 不合法的参数
        if data['resultcode'] == 200:
            # 记录列表
            res = data['records']
    return res


def find_data_requests(self, level):
    requests = {}
    api_path = self.get_level_meta_data(level)['api_path']
    api_key = api_path.replace("http://webapi.cninfo.com.cn", '')
    for r in self.driver.requests:
        if r.method == 'POST' and api_key in r.path:
            requests[r.url] = r
    return requests.values()


def read_json_data(self, level):
    """解析查询数据"""
    data = []
    requests = find_data_requests(self, level)
    for r in requests:
        data.extend(parse_response(self, r))
    # 删除已经读取的请求
    del self.driver.requests
    return data
