# fancyMySQL
 fancyMySQL，花式玩转数据库,文件写入数据库，文件写入mysql~ 数据库可以当网盘用啦。

# 安装依赖

1、安装MySQL-python依赖：https://cpp.la/145.html#i-4    
2、sudo pip install -r requirements.txt　　　　

# 使用帮助

１、编辑setting.py中的数据库信息。　　　　

２、上传：

python fancyMySQL.py put $fileName

３、下载：

python fancyMySQL.py get $fileName $filePath　

4、目录：

python fancyMySQL.py tree        　　

# 更新说明　　　　

20180504: version beta 1.3, 上传文件块完整度检查; 加入tree命令，文件目录展示
20180425: version beta 1.2, 加入多线程下载，类似IDM
20180424: version beta 1.1, 自动创建表, 加入多线程上传    



# 异常问题参看
  https://stackoverflow.com/questions/25865270/how-to-install-python-mysqldb-module-using-pip    
  https://cpp.la/145.html

# 未完待续,完善后会封包
