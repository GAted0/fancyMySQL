# coding: utf-8

__author__='https://cpp.la'

import MySQLdb
import time
import sys
import gc
import threading
from tqdm import tqdm
from warnings import filterwarnings
from setting import MYSQL_URI, OPEN_THREAD, MAX_THREAD
filterwarnings('ignore', category=MySQLdb.Warning)

# version: beta 1.3
# todo:　包不完整，重试机制
# todo: 未优化拆包内存：　上传需要占用同等文件大小的内存。 [下次重点优化,边拆边发]
# todo: 未作分表存储（一直单表存储数据会导致目标服务器负载高）,分表聚合索引mediaIndex。


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
            print("连接数据库出错!")

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

def putFileThread(fileName, fileChunk):
    '''
    :param fileName: 文件名
    :param fileChunk:　待上传的单个文件块
    :return:
    '''
    MC = mysql_client()
    keyContent = fileChunk[0]
    valueContent = fileChunk[1]
    if keyContent != "splitCount":
        MC.writeBLOB(fileName=fileName, chunkID=int(keyContent), binaryBuffer=valueContent)
    else:
        MC.writeBLOB(fileName=fileName, chunkID=int(0), binaryBuffer=bytes(valueContent))
    del valueContent
    del keyContent
    del MC
    gc.collect()

def putFileWork(fileName, bigChunkedFileDict, maxThread=MAX_THREAD):
    '''
    :param fileName: 文件名
    :param bigChunkedFileDict:　拆分的整个文件块
    :param maxThread: 最大线程
    :return:
    '''
    while len(bigChunkedFileDict):
        if threading.activeCount() < maxThread:
            t = threading.Thread(target=putFileThread, kwargs={'fileName': fileName, 'fileChunk': bigChunkedFileDict.popitem()})
            t.setDaemon(True)
            t.start()
        else:
            time.sleep(0.1)
    currentThread = threading.current_thread()
    for t in threading.enumerate():
        if t is currentThread:
            continue
        else:
            t.join()

def getFileThread(fileName, filePath, singleChunk):
    '''
    :param fileName: 文件名
    :param filePath:　文件写入路径
    :param singleChunk:　待下载的文件索引块
    :return:
    '''
    MC = mysql_client()
    with open(filePath, "rb+") as f:
        f.seek((singleChunk-1)*1024*1024)
        f.write(MC.readBLOB(fileName, singleChunk))
    del MC
    gc.collect()

def getFileWork(fileName, filePath, splitCount, maxThread=MAX_THREAD):
    '''
    :param fileName:　文件名
    :param filePath:　文件写入路径
    :param splitCount:　文件总索引块数量
    :param maxThread:　最大线程
    :return:
    '''
    while splitCount:
        if threading.activeCount() < maxThread:
            t = threading.Thread(target=getFileThread, kwargs={'fileName': fileName, 'filePath': filePath, 'singleChunk': splitCount})
            splitCount = splitCount - 1
            t.setDaemon(True)
            t.start()
        else:
            time.sleep(0.1)
    currentThread = threading.currentThread()
    for t in threading.enumerate():
        if t is currentThread:
            continue
        else:
            t.join()

def getFileList():
    MC = mysql_client()
    sql = 'select distinct(name) from media'
    for i_index, i in enumerate(MC.read_sql(sql)):
        print("%d: %s" % (i_index+1,i[0]))
    del MC
    gc.collect()

def _checkPackageComplete(fileName):
    MC = mysql_client()
    sql = 'select file from media where name="%s" and chunkID=0' % fileName
    splitCount = int(MC.read_sql(sql)[0][0])
    sql = 'select count(name) from media where name="%s"' % fileName
    writeCount = int(MC.read_sql(sql)[0][0])
    if splitCount == writeCount-1:
        return True
    else:
        return False

def _checkPackageExist(fileName):
    MC = mysql_client()
    sql = 'select count(id) from media where name="%s" and chunkID=0' % fileName
    isExist = int(MC.read_sql(sql)[0][0])
    del MC
    gc.collect()

    if isExist == 0:
        return False
    else:
        return True


if __name__ == '__main__':
    helpStr = '''
    [帮助] help: python fancyMysql.py help
    [上传] put: python fancyMysql.py put $filePath
    [下载] get: python fancyMysql.py get $fileName $filePath 
    [目录] tree: python fancyMysql.py tree
    '''
    if len(sys.argv) < 2:
        raise Exception("Parameter Exception!")

    createTable()

    method = sys.argv[1]
    if method not in ["help", "put", "get", "tree"]:
        raise Exception("help[帮助] or put[上传] or get[下载] or tree[目录] File?")

    elif method == "help":
        print(helpStr)

    elif method == "put":
        filePath = sys.argv[2]
        fileName = filePath.split("/")[-1]
        if _checkPackageExist(fileName) is True:
            print('文件已存在!')
            sys.exit(0)

        startTime = time.time()
        FH = file_handle(filePath)

        if OPEN_THREAD:
            putFileWork(fileName, FH.splitFile)
        else:
            MC = mysql_client()
            for k, v in tqdm(FH.splitFile.items()):
                if k != "splitCount":
                    MC.writeBLOB(fileName=fileName, chunkID=int(k), binaryBuffer=v)
                else:
                    MC.writeBLOB(fileName=fileName, chunkID=int(0), binaryBuffer=bytes(v))
            del MC

        if _checkPackageComplete(fileName) is False:
            print('上传失败, 完整性校验失败!')
        else:
            print('上传成功, 完整性校验成功!')

        del FH
        gc.collect()
        endTime = time.time()
        print("耗时: %s秒" % int(endTime-startTime))

    elif method == "get":
        fileName = sys.argv[2]
        filePath = sys.argv[3]
        if _checkPackageExist(fileName) is False:
            print('文件不存在!')
            sys.exit(0)

        startTime = time.time()

        MC = mysql_client()
        sql = 'select file from media where name="%s" and chunkID=0' % fileName
        splitCount = int(MC.read_sql(sql)[0][0])

        if OPEN_THREAD:
            lastChunkBytes = len(MC.readBLOB(fileName, splitCount))
            allChunkBytes = (splitCount-1) * 1024 * 1024 + lastChunkBytes
            with open(filePath, "wb") as f:
                f.truncate(allChunkBytes)
            getFileWork(fileName, filePath, splitCount)
        else:
            with open(filePath, "wb") as f:
                for i in tqdm(range(1, splitCount + 1)):
                    f.write(MC.readBLOB(fileName, i))

        endTime = time.time()
        print("耗时: %s秒" % int(endTime - startTime))

    elif method == "tree":
        getFileList()


