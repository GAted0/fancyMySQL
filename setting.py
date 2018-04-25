# coding: utf-8

__author__='https://cpp.la'

# 花式数据库的配置
# 好车不多，开车需珍惜：线程推荐不要超过10-20,防止目标服务器负载过高

# 数据库信息
MYSQL_URI = {
    'db': 'db',
    'host': 'host',
    'port': 3306,
    'user': 'root',
    'password': '123456'
}

#　多线程下载已有人pm有bug，调试中~
# 是否开启多线程, False, True
OPEN_THREAD=False

# 最大线程数
MAX_THREAD=10