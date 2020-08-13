import pymysql
import re
from DBUtils.PooledDB import PooledDB

drop_counts = 0


class MysqlDB:
    def __init__(self, host="129.211.145.88", port=3306, dbname="funddb", user="root", pwd="1qaz!QAZ"):
        self.__host = host
        self.__port = port
        self.__dbname = dbname
        self.__user = user
        self.__pwd = pwd
        self.__pool = self.__createPool()

    def __createPool(self):
        pool = PooledDB(
            creator=pymysql,  # 使用链接数据库的模块
            maxconnections=0,  # 连接池允许的最大连接数，0和None表示不限制连接数
            mincached=2,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
            maxcached=5,  # 链接池中最多闲置的链接，0和None不限制
            maxshared=3,
            # 链接池中最多共享的链接数量，0和None表示全部共享。PS: 无用，因为pymysql和MySQLdb等模块的
            # threadsafety都为1，所有值无论设置为多少，_maxcached永远为0，所以永远是所有链接都共享。
            blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
            maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
            setsession=[],  # 开始会话前执行的命令列表。
            ping=0,  # ping MySQL服务端，检查是否服务可用。
            host=self.__host,
            port=self.__port,
            user=self.__user,
            password=self.__pwd,
            database=self.__dbname
            # charset='utf8'
        )
        return pool

    def getConnFromPool(self):
        return self.__pool.connection()


def tableExists(conn, table_name):
    sql = "show tables;"
    cursor = conn.cursor()
    cursor.execute(sql)
    tables = [cursor.fetchall()]
    cursor.close()
    table_list = re.findall('(\'.*?\')', str(tables))
    table_list = [re.sub("'", '', each) for each in table_list]
    if table_name in table_list:
        return True
    else:
        return False


def createFundListTable(conn):
    cursor = conn.cursor()
    sql = "create table fund_list (" \
          "id INT auto_increment PRIMARY KEY," \
          "fund_code CHAR(6) NOT NULL UNIQUE," \
          "fund_short_name VARCHAR(50) NOT NULL," \
          "fund_type VARCHAR(20) NOT NULL" \
          ")"
    try:
        cursor.execute(sql)
    except Exception as e:
        print(e)
    cursor.close()


def createFundInfoTable(conn):
    cursor = conn.cursor()
    sql = "create table fund_info_table (" \
          "id INT auto_increment PRIMARY KEY," \
          "fund_code CHAR(6) NOT NULL UNIQUE," \
          "fund_full_name VARCHAR(50) NOT NULL," \
          "fund_short_name VARCHAR(50) NOT NULL," \
          "fund_type VARCHAR(20) NOT NULL," \
          "fund_issue_date DOUBLE ," \
          "fund_issue_date_string VARCHAR(50)," \
          "fund_launch_date DOUBLE ," \
          "fund_launch_date_string VARCHAR(50)," \
          "fund_asset_size DOUBLE," \
          "fund_company VARCHAR(50) NOT NULL," \
          "fund_trustee VARCHAR(50) NOT NULL," \
          "fund_manager VARCHAR(50) NOT NULL," \
          "fund_dividend_payment_per_unit DOUBLE," \
          "fund_dividend_amounts INT," \
          "fund_purchase_status VARCHAR(50)," \
          "fund_redemption_status VARCHAR(50)" \
          ")"
    try:
        cursor.execute(sql)
    except Exception as e:
        print(e)
    cursor.close()


def createFundHistoryTable(conn, fund_code, is_net_asset_value):
    cursor = conn.cursor()
    table_name = "history_" + fund_code + "_table"
    if is_net_asset_value:
        sql = "create table " + table_name + " (" \
                                             "id INT auto_increment," \
                                             "date_timestamp DOUBLE NOT NULL," \
                                             "date_string VARCHAR(50) NOT NULL," \
                                             "net_asset_value DOUBLE, " \
                                             "accumulated_net_asset_value DOUBLE, " \
                                             "PRIMARY KEY (id, date_timestamp)" \
                                             ")"
    else:
        sql = "create table " + table_name + " (" \
                                             "id INT auto_increment," \
                                             "date_timestamp DOUBLE NOT NULL," \
                                             "date_string VARCHAR(50) NOT NULL," \
                                             "earnings_per_10000 DOUBLE, " \
                                             "7_day_annual_return DOUBLE, " \
                                             "PRIMARY KEY (id, date_timestamp)" \
                                             ")"
    try:
        cursor.execute(sql)
    except Exception as e:
        print(e)
    cursor.close()


def createFundNoHistory(conn):
    cursor = conn.cursor()
    sql = "create table fund_no_history_table (" \
          "id INT auto_increment PRIMARY KEY," \
          "fund_code CHAR(6) NOT NULL UNIQUE" \
          ")"
    try:
        cursor.execute(sql)
    except Exception as e:
        print(e)
    cursor.close()


def existsInFundNoHistoryTable(conn, fund_code):
    cursor = conn.cursor()
    sql = "select * from fund_no_history_table where fund_code='" + fund_code + "'"
    try:
        cursor.execute(sql)
    except Exception as e:
        print(e)
    count = cursor.rowcount
    cursor.close()
    if count == 0:
        return False
    else:
        return True


def removeFromFundNoHistoryTable(conn, fund_code):
    cursor = conn.cursor()
    sql = "delete from fund_no_history_table where fund_code='" + fund_code + "'"
    try:
        cursor.execute(sql)
        conn.commit()
    except Exception as e:
        print(e)
        conn.rollback()
    cursor.close()


def truncateTable(conn, table_name):
    cursor = conn.cursor()
    sql = "truncate table " + table_name
    try:
        cursor.execute(sql)
        cursor.close()
        return True
    except Exception as e:
        print(e)
        return False


def dropTable(conn, table_name):
    cursor = conn.cursor()
    sql = "drop table " + table_name
    try:
        cursor.execute(sql)
        cursor.close()
        return True
    except Exception as e:
        print(e)
        return False


def selectAllHistoryTables(conn):
    cursor = conn.cursor()
    sql = "SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'history_%_table';"
    cursor.execute(sql)
    results = cursor.fetchall()
    cursor.close()
    history_tables_list = []
    for result in results:
        history_tables_list.append(result[0])
    return history_tables_list


def dropAllHistoryTables(conn, all_history_tables, start_index=0, end_index=0):
    all_history_tables_size = len(all_history_tables)
    if end_index == 0:
        end_index = all_history_tables_size
    cursor = conn.cursor()
    for i in range(start_index, end_index):
        sql = "drop table " + all_history_tables[i] + ";"
        cursor.execute(sql)
        global drop_counts
        drop_counts += 1
        print("%s / %s" % (drop_counts, all_history_tables_size))
    cursor.close()
    conn.close()  # 多线程删除，每个线程运行完后断开连接


if __name__ == "__main__":
    db = MysqlDB()
    conn = db.getConnFromPool()
    result = selectAllHistoryTables(conn)
    print(result)
    conn.close()

    # cursor = conn.cursor()
    # sql = "select fund_type from fund_list_table"
    # try:
    #     cursor.execute(sql)
    # except Exception as e:
    #     print(e)
    #
    # results = cursor.fetchall()
    # fund_type_names = []
    # for result in results:
    #     if result[0] not in fund_type_names:
    #         fund_type_names.append(result[0])
    #
    # sql = "insert into fund_type_table(fund_type_name) values(%s)"
    #
    # try:
    #     cursor.executemany(sql, fund_type_names)
    #     conn.commit()
    # except Exception as e:
    #     print(e)
    #     conn.rollback()
    #
    # cursor.close()
    # conn.close()
    #
    # print(fund_type_names)
