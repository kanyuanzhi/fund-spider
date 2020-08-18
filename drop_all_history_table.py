from util.mysql_util import MysqlDB, dropAllHistoryTables, selectAllHistoryTableNames, truncateTable
from util.thread_util import countIndexGroups
import threading
from time import time


class OneThread(threading.Thread):
    def __init__(self, thread_id, start_index, end_index, conn, all_history_tables):
        super(OneThread, self).__init__()
        self.__thread_id = thread_id
        self.__start_index = start_index
        self.__end_index = end_index
        self.__conn = conn
        self.__all_history_tables = all_history_tables

    def run(self):
        dropAllHistoryTables(self.__conn, self.__all_history_tables, self.__start_index, self.__end_index)


def runThread(thread_numbers):
    start_time = time()

    db = MysqlDB()
    conn = db.getConnFromPool()
    truncateTable(conn, "fund_no_history_table")
    all_history_tables = selectAllHistoryTableNames(conn)
    conn.close()

    all_history_tables_size = len(all_history_tables)
    index_groups = countIndexGroups(thread_numbers, all_history_tables_size)

    threads = []
    for i in range(thread_numbers):
        conn = db.getConnFromPool()
        threads.append(OneThread(i, index_groups[i][0], index_groups[i][1], conn, all_history_tables))

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    end_time = time()
    print("共删除%s张历史记录表，清空fund_no_history_table，耗时%.2fs." % (all_history_tables_size, end_time - start_time))


if __name__ == "__main__":
    runThread(20)
