# coding: utf-8

__author__='https://cpp.la'

# 花式数据库的配置

# 数据库信息
MYSQL_URI = {
    'db': 'db',
    'host': 'host',
    'port': 3306,
    'user': 'root',
    'password': '123456'
}
# 是否开启多线程
OPEN_THREAD=False
# 最大线程数：推荐10以下，最大20。开车需珍惜
MAX_THREAD=10