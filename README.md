# fancyMySQL
 fancyMySQL，花式玩转数据库~　数据库可以当网盘用啦。

# 安装依赖

sudo pip install -r requirements.txt


# 使用帮助

１、创建表:

CREATE TABLE `media` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(128) DEFAULT '',
  `chunkID` int(11) NOT NULL,
  `file` longblob,
  PRIMARY KEY (`id`),
  KEY `NewIndex1` (`name`),
  KEY `NewIndex2` (`chunkID`)
)

２、上传：

python fancyMySQL.py put $fileName

３、下载：

python fancyMySQL.py get $fileName $filePath


# 异常问题参看
# https://stackoverflow.com/questions/25865270/how-to-install-python-mysqldb-module-using-pip
# https://cpp.la/145.html

# 未完待续,完善后会封包