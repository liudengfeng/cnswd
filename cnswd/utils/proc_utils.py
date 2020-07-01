import psutil


def find_procs_by_name(name):
    "Return a list of processes matching 'name'."
    ls = []
    for p in psutil.process_iter(attrs=['name']):
        if p.info['name'] == name:
            ls.append(p)
    return ls


def before_refresh():
    """某些进程可能阻碍提取数据"""
    for e in ['lantern.exe']:
        for p in find_procs_by_name(e):
            try:
                p.kill()
            except psutil.NoSuchProcess:
                pass


def kill_firefox():
    """杀死可能残留的firefox进程"""
    for e in ['geckodriver.exe', 'firefox.exe']:
        for p in find_procs_by_name(e):
            try:
                p.kill()
            except psutil.NoSuchProcess:
                pass
