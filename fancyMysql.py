# coding: utf-8

__author__='https://cpp.la'

import MySQLdb
import time
import sys
import gc
from tqdm import tqdm
from warnings import filterwarnings
filterwarnings('ignore', category=MySQLdb.Warning)

# version: beta 1.0
# todo: 未优化内存：　上传需要占用同等文件大小的内存（拆包）。
# todo: 分表索引聚合表mediaIndex
# todo: 未作分表存储（一直单表存储数据会导致目标服务器负载高）。
# todo: 多线程未完

# 数据库信息
MYSQL_URI = {
    'db': 'db',
    'host': 'host',
    'port': 3306,
    'user': 'root',
    'password': '123456'
}

class mysql_client(object):

    def __init__(self):
        try:
            self.mysql = MySQLdb.connect(
                db = MYSQL_URI.get('db'),
                host = MYSQL_URI.get('host'),
                user = MYSQL_URI.get('user'),
                passwd = MYSQL_URI.get('password'),
                port = MYSQL_URI.get('port'),
                local_infile=True
            )
        except Exception as e:
            print(str(e))

    def __del__(self):
        self.mysql.close()

    def exec_sql(self, sql):
        cur = self.mysql.cursor()
        try:
            cur.execute(sql)
            self.mysql.commit()
            return True
        except Exception as e:
            self.mysql.rollback()
            return False

    def read_sql(self, sql):
        cur = self.mysql.cursor()
        cur.execute(sql)
        return list(cur.fetchall())

    def escape_parameter(self, parameter):
        if isinstance(parameter, str):
            return MySQLdb.escape_string(parameter)
        else:
            return MySQLdb.escape(parameter)

    def writeBLOB(self, fileName, chunkID, binaryBuffer):
        cur = self.mysql.cursor()
        sql = 'insert into media(name, chunkID, file) values("' + fileName +'",' + str(chunkID) + ', _binary %s)'
        try:
            cur.execute(sql, (binaryBuffer, ))
            self.mysql.commit()
            return True
        except:
            self.mysql.rollback()
            return False

    def readBLOB(self, fileName, chunkID):
        sql = 'select file from media where name="%s" and chunkID=%d' %(fileName, chunkID)
        cur = self.mysql.cursor()
        cur.execute(sql)
        return cur.fetchall()[0][0]

class file_handle(object):

    def __init__(self, filePath):
        self.filePath = filePath
        self.memoryBuffer = {}
        self.splitSize = 1024*1024
        self.splitCount = 0

    @property
    def splitFile(self):
        with open(self.filePath, "rb") as f:
            while True:
                chunk = f.read(self.splitSize)
                if chunk:
                    self.splitCount += 1
                    self.memoryBuffer[str(self.splitCount)] = chunk
                else:
                    self.memoryBuffer["splitCount"] = self.splitCount
                    break
        return self.memoryBuffer

def createTable():
    MC = mysql_client()
    sql = '''
    CREATE TABLE IF NOT EXISTS media(
        id int(11) NOT NULL AUTO_INCREMENT,
        name varchar(128) DEFAULT '',
        chunkID int(11) NOT NULL,
        file longblob,
        PRIMARY KEY (id),
        KEY NewIndex1 (name),
        KEY NewIndex2 (chunkID)
    )
    '''
    if MC.exec_sql(sql) is False:
        raise Exception("create Table media failed!")
    else:
        pass
    del MC
    gc.collect()

if __name__ == '__main__':
    '''
    put: python fancyMysql.py put $filePath
    get: python fancyMysql.py get $fileName $filePath 
    '''
    if len(sys.argv) < 3:
        raise Exception("Parameter Exception!")

    createTable()

    method = sys.argv[1]

    if method != "put" and method != "get":
        raise Exception("Put or Get File?")

    elif method == "put":
        filePath = sys.argv[2]
        fileName = filePath.split("/")[-1]

        FH = file_handle(filePath)
        MC = mysql_client()

        for k,v in tqdm(FH.splitFile.items()):
            if k!= "splitCount":
                MC.writeBLOB(fileName=fileName, chunkID=int(k), binaryBuffer=v)
            else:
                MC.writeBLOB(fileName=fileName, chunkID=int(0), binaryBuffer=bytes(v))

        del FH
        del MC
        gc.collect()

    elif method == "get":
        fileName = sys.argv[2]
        filePath = sys.argv[3]

        MC = mysql_client()
        sql = 'select file from media where name="%s" and chunkID=0' % fileName
        spilitCount = int(MC.read_sql(sql)[0][0])

        with open(filePath, "wb") as f:
            for i in tqdm(range(1, spilitCount + 1)):
                f.write(MC.readBLOB(fileName, i))


    # todo: 这里是未完成的代码
    # target = ''
    # p = Pool(MAX_PROCESS)
    # p.apply_async(update_progress, (i,))
    # p.close()
    # p.join()

