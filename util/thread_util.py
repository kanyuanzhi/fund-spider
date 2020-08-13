import threading
from time import time
from util.mysql_util import MysqlDB, tableExists, truncateTable, createFundInfoTable
from crawl_fund_list import getFundCodes


def countIndexGroups(thread_numbers, fund_codes_size):
    unit_size = int(fund_codes_size / thread_numbers)
    start_index = 0
    end_index = start_index + unit_size
    index_groups = []
    for i in range(thread_numbers):
        index_groups.append([start_index, end_index])
        start_index = end_index
        end_index = end_index + unit_size
    index_groups[-1][1] = fund_codes_size
    return index_groups


class OneThread(threading.Thread):
    def __init__(self, thread_id, start_index, end_index, spider):
        super(OneThread, self).__init__()
        self.__thread_id = thread_id
        self.__start_index = start_index
        self.__end_index = end_index
        self.__one_spider = spider

    def run(self):
        self.__one_spider.crawlAndSave(start_index=self.__start_index, end_index=self.__end_index)


def runThreads(Spider, thread_numbers):
    db = MysqlDB()  # 多线程爬取，每个线程从连接池中获取数据库连接
    fund_codes = getFundCodes()

    if Spider.__name__ == "FundInfoSpider":
        conn = db.getConnFromPool()
        if tableExists(conn, "fund_info_table"):
            truncateTable(conn, "fund_info_table")
        else:
            createFundInfoTable(conn)
        conn.close()

    fund_codes_size = len(fund_codes)
    # fund_codes_size = 50

    start_time = time()
    index_groups = countIndexGroups(thread_numbers, fund_codes_size)
    threads = []
    for i in range(thread_numbers):
        conn = db.getConnFromPool()
        spider = Spider(conn, fund_codes)
        threads.append(OneThread(i, index_groups[i][0], index_groups[i][1], spider))

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    end_time = time()
    print("%s完成，耗时%.2fs." % (Spider.__name__, end_time - start_time))
