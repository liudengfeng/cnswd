class CSS:
    menu_root = '.nav-tab-menu'  # Class Selector
    check_loaded = '.nav-title'
    sdate = 'input.date:nth-child(1)'
    edate = 'input.date:nth-child(2)'
    clear_date = '.datepicker-days > table:nth-child(1) > tfoot:nth-child(3) > tr:nth-child(2) > th:nth-child(1)'
    select_year = '#se1_sele'
    select_quarter = 'div.box-filter:nth-child(2) > select:nth-child(2)'
    # 子类重写
    query_btn = ''
    # 输入过滤条件模板
    condition_fmt = 'div[class$="condition{}"]'


class DbCss(CSS):
    query_btn = 'button.box-filter'
    data_loaded = '.onloading'
    market_css = '.classify-tree > li:nth-child(6)'
    market_code_fmt = 'a[data-id="{}"]'
    all_input_code = '.cont-top-right > div:nth-child(1) > div:nth-child(1) > label:nth-child(1)'
    add_all_code_btn = '.cont-top-right > div:nth-child(2) > button:nth-child(1)'
    to_select_code = '.cont-top-right > div:nth-child(1) > div:nth-child(1) > span:nth-child(2) > i:nth-child(1)'
    selected_code = '.cont-top-right > div:nth-child(3) > div:nth-child(1) > span:nth-child(2) > i:nth-child(1)'
    add_all_field = '.detail-cont-bottom > div:nth-child(1) > div:nth-child(1) > label:nth-child(1)'
    add_all_field_btn = '.detail-cont-bottom > div:nth-child(2) > button:nth-child(1)'
    selected_field = '.detail-cont-bottom > div:nth-child(3) > div:nth-child(1) > span:nth-child(2) > i:nth-child(1)'


class TsCss(CSS):
    query_btn = 'button.box-filter:nth-child(11)'
    input_code = '#input_code'
    search_code = 'div.searchDataRes:nth-child(2)'
    sdate = '#fBDatepair > input:nth-child(1)'
    data_loaded = '.fixed-table-loading'
