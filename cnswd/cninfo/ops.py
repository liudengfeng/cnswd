"""

通用操作

"""
import pandas as pd
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait

from ..setting.config import POLL_FREQUENCY, TIMEOUT


def _normalize_level_num(x):
    """修正菜单层级数字"""
    assert int(x) <= 11, '菜单子级数值最大不超过11'
    if x == '10':
        return 'a'
    elif x == '11':
        return 'b'
    else:
        return x


def _navigate(driver, nums):
    num = [_normalize_level_num(x) for x in nums]
    num = ''.join(num)
    xpath = f'//li[@data-pos="{num}"]'
    li = driver.find_element_by_xpath(xpath)
    li.find_element_by_tag_name('a').click()


def navigate(driver, level):
    """导航顶部菜单

    Arguments:
        driver {driver}} -- 无头浏览器
        level {str} -- 菜单层级（以`.`分隔）
    """
    ls = level.split('.')
    for i in range(1, len(ls) + 1):
        nums = ls[:i]
        _navigate(driver, nums)
        driver.implicitly_wait(0.1)


def toggler_open(elem):
    """展开元素"""
    assert elem.tag_name == 'li', '必须为"li"元素'
    attr = elem.get_attribute('class')
    if 'closed' in attr:
        span = elem.find_element_by_tag_name('span')
        span.click()


def toggler_close(elem):
    """折叠或隐藏元素"""
    assert elem.tag_name == 'li', '必须为"li"元素'
    attr = elem.get_attribute('class')
    if 'opened' in attr:
        span = elem.find_element_by_tag_name('span')
        span.click()


class element_attribute_change_to(object):
    """期望元素属性更改为指定值或值的一部分。
    """
    def __init__(self, locator, name, attribute, not_in=False):
        """
        Arguments:
            locator {定位元组} -- 定位对象  
            name {str} -- 元素属性名称
            attribute {str} -- 属性值

        Keyword Arguments:
            not_in {bool} -- 属性是否不在元素值中
        """
        self.locator = locator
        self.name = name
        self.attribute = attribute
        self.not_in = not_in

    def __call__(self, driver):
        # Finding the referenced element
        element = driver.find_element(*self.locator)
        if self.not_in:
            if self.attribute not in element.get_attribute(self.name):
                return element
            else:
                return False
        else:
            # 当指定名称的属性变更为指定属性时，返回该元素
            if self.attribute in element.get_attribute(self.name):
                return element
            else:
                return False


class element_text_change_to(object):
    """期望元素文本为指定条件时
    """
    def __init__(self, locator, text, not_in=False):
        """
        Arguments:
            locator {定位元组} -- 定位对象  
            text {str} -- 元素文本内容

        Keyword Arguments:
            not_in {bool} -- 指定的值是否属于文本
        """
        self.locator = locator
        self.text = text
        self.not_in = not_in

    def __call__(self, driver):
        # Finding the referenced element
        element = driver.find_element(*self.locator)
        if self.not_in:
            if self.text not in element.text:
                return element
            else:
                return False
        else:
            if self.text in element.text:
                return element
            else:
                return False


def wait_for_preview(wait, wait_css, part_style, msg, not_in=False):
    """等待结果呈现"""
    # 以属性值改变来判断
    locator = (By.CSS_SELECTOR, wait_css)
    wait.until(
        element_attribute_change_to(locator, 'style', part_style, not_in), msg)


def wait_page_loaded(wait, elem_css, text, msg, not_in=False):
    """
    等待网页加载完成
    """
    locator = (By.CSS_SELECTOR, elem_css)
    wait.until(element_text_change_to(locator, text, not_in), msg)


def read_html_table(driver, num, attrs):
    """读取指定页的数据表"""
    assert 'id' in attrs.keys(), '必须指定id属性'
    na_values = ['-', '无', ';']
    driver.find_element_by_link_text(str(num)).click()
    df = pd.read_html(driver.page_source, na_values=na_values, attrs=attrs)[0]
    return df


def wait_for_visibility(driver, elem_css, msg=''):
    """
    等待指定css元素可见

    Arguments:
        elem_css {str} -- 可见元素的css表达式
    """
    m = EC.visibility_of_element_located((By.CSS_SELECTOR, elem_css))
    wait = WebDriverWait(driver, TIMEOUT, POLL_FREQUENCY)
    wait.until(m, message=msg)


def wait_for_invisibility(driver, elem_css, msg=''):
    """
    等待指定css元素不可见

    Arguments:
        elem_css {str} -- 可见元素的css表达式
    """
    m = EC.invisibility_of_element((By.CSS_SELECTOR, elem_css))
    wait = WebDriverWait(driver, TIMEOUT, POLL_FREQUENCY)
    return wait.until(m, message=msg)


def wait_for_activate(driver, data_name, status='active'):
    """等待元素激活"""
    xpath_fmt = "//a[@data-name='{}']"
    locator = (By.XPATH, xpath_fmt.format(data_name))
    wait = WebDriverWait(driver, TIMEOUT, POLL_FREQUENCY)
    wait.until(element_attribute_change_to(locator, 'class', status),
               f'{data_name} 激活元素超时')


def wait_for_all_presence(driver, elem_css, msg=''):
    """
    等待指定css的所有元素出现

    Arguments:
        elem_css {str} -- 元素的css表达式
    """
    m = EC.presence_of_all_elements_located((By.CSS_SELECTOR, elem_css))
    wait = WebDriverWait(driver, TIMEOUT, POLL_FREQUENCY)
    wait.until(m, message=msg)


def datepicker(driver, css, date_str):
    """指定日期"""
    elem = driver.find_element_by_css_selector(css)
    # 首次加载网页在某些时候可能不可见
    # wait_for_visibility(driver, css)
    elem.click()
    elem.clear()
    # 需要tab转移光标确定日期
    # actions = ActionChains(driver)
    # actions = actions.send_keys(Keys.TAB)
    elem.send_keys(date_str, Keys.TAB)


def change_year(driver, css, year):
    """改变查询指定id元素的年份"""
    elem = driver.find_element_by_css_selector(css)
    # wait_for_visibility(driver, css)
    elem.clear()
    elem.send_keys(str(year))
