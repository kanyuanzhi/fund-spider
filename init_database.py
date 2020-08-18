# 初始化数据表

from util.mysql_util import MysqlDB, tableExists

SQL_CREATE_FUND_LIST_TABLE = "create table fund_list (" \
                             "id INT auto_increment PRIMARY KEY," \
                             "fund_code CHAR(6) NOT NULL UNIQUE," \
                             "fund_short_name VARCHAR(50) NOT NULL," \
                             "fund_type VARCHAR(20) NOT NULL" \
                             ")"

SQL_CREATE_FUND_INFO_TABLE = "create table fund_info_table (" \
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

SQL_CREATE_FUND_NO_HISTORY_TABLE = "create table fund_no_history_table (" \
                                   "id INT auto_increment PRIMARY KEY," \
                                   "fund_code CHAR(6) NOT NULL UNIQUE" \
                                   ")"

SQL_CREATE_FUND_LATEST_LOG_TABLE = "create table crawl_fund_latest_log_table (" \
                                   "id INT auto_increment PRIMARY KEY," \
                                   "fund_code CHAR(6) NOT NULL UNIQUE," \
                                   "fund_short_name VARCHAR(50) NOT NULL," \
                                   "fund_type VARCHAR(20) NOT NULL" \
                                   ")"

SQL_CREATE_FUND_TYPE_TABLE = "create table fund_type_table (" \
                             "id INT auto_increment PRIMARY KEY," \
                             "fund_type VARCHAR(20) NOT NULL" \
                             ")"


def createTableTemplate(conn, sql):
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
    except Exception as e:
        print(e)
    cursor.close()


def createFundListTable(conn):
    if not tableExists(conn, "fund_list_table"):
        createTableTemplate(conn, SQL_CREATE_FUND_LIST_TABLE)


def createFundInfoTable(conn):
    if not tableExists(conn, "fund_info_table"):
        createTableTemplate(conn, SQL_CREATE_FUND_INFO_TABLE)


def createFundNoHistoryTable(conn):
    if not tableExists(conn, "fund_no_history_table"):
        createTableTemplate(conn, SQL_CREATE_FUND_NO_HISTORY_TABLE)


def createCrawlFundLatestLogTable(conn):
    if not tableExists(conn, "crawl_fund_latest_log_table"):
        createTableTemplate(conn, SQL_CREATE_FUND_LATEST_LOG_TABLE)


def createFundTypeTable(conn):
    if not tableExists(conn, "fund_type_table"):
        createTableTemplate(conn, SQL_CREATE_FUND_TYPE_TABLE)


def createAllTables():
    db = MysqlDB
    conn = db.getConnFromPool()
    createFundListTable(conn)
    createFundInfoTable(conn)
    createFundNoHistoryTable(conn)
    createCrawlFundLatestLogTable(conn)
    conn.close()
