# 安装包
```python
pip install pytest-xdist
pip install pytest-html
```

# 运行测试

## 设置
+ pytest.ini 文件中添加以下内容
```
[pytest]
addopts=-n8
```
+ 运行
```
# 浏览器打开report.html查看测试报告
pytest <target-test-file> --html=report.html
```