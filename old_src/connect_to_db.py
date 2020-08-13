import pymysql
import re


class MysqlDB:
    def __init__(self, host="129.211.145.88", port=3306, dbname="funddb", user="root", pwd="1qaz!QAZ"):
        self._host = host
        self._port = port
        self._dbname = dbname
        self._user = user
        self._pwd = pwd
        self._conn = self._getConn()

    def _getConn(self):
        if self.dbExists():
            try:
                conn = pymysql.connect(
                    host=self._host,
                    port=self._port,
                    user=self._user,
                    password=self._pwd,
                    database=self._dbname,
                    charset="utf8")
            except Exception as e:
                print(e)
                return False
            return conn
        else:
            return False

    def getConn(self):
        return self._conn

    def dbExists(self):
        # 判断数据库是否存在
        try:
            conn = pymysql.connect(
                host=self._host,
                port=self._port,
                user=self._user,
                password=self._pwd,
                database="information_schema",
                charset="utf8")
        except Exception as e:
            print(e)
            return
        cursor = conn.cursor()
        sql = "select * from SCHEMATA where SCHEMA_NAME=\'" + self._dbname + "\';"
        cursor.execute(sql)
        count = cursor.rowcount
        cursor.close()
        conn.close()
        if count == 1:
            return True
        else:
            return False

    def tableExists(self, table_name):
        sql = "show tables;"
        cursor = self._conn.cursor()
        cursor.execute(sql)
        tables = [cursor.fetchall()]
        cursor.close()
        table_list = re.findall('(\'.*?\')', str(tables))
        table_list = [re.sub("'", '', each) for each in table_list]
        if table_name in table_list:
            return True
        else:
            return False

    def createFundListTable(self):
        cursor = self._conn.cursor()
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

    def createFundInfoTable(self):
        cursor = self._conn.cursor()
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

    def createFundHistoryTable(self, fund_code, is_net_asset_value):
        cursor = self._conn.cursor()
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

    def createFundNoHistory(self):
        cursor = self._conn.cursor()
        sql = "create table fund_no_history_table (" \
              "id INT auto_increment PRIMARY KEY," \
              "fund_code CHAR(6) NOT NULL UNIQUE" \
              ")"
        try:
            cursor.execute(sql)
        except Exception as e:
            print(e)
        cursor.close()

    def existsInFundNoHistoryTable(self, fund_code):
        cursor = self._conn.cursor()
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

    def removeFromFundNoHistoryTable(self, fund_code):
        cursor = self._conn.cursor()
        sql = "delete from fund_no_history_table where fund_code='" + fund_code + "'"
        try:
            cursor.execute(sql)
            self._conn.commit()
        except Exception as e:
            print(e)
            self._conn.rollback()
        cursor.close()

    def truncateTable(self, table_name):
        cursor = self._conn.cursor()
        sql = "truncate table " + table_name
        try:
            cursor.execute(sql)
            cursor.close()
            return True
        except Exception as e:
            print(e)
            return False

    def dropTable(self, table_name):
        cursor = self._conn.cursor()
        sql = "drop table " + table_name
        try:
            cursor.execute(sql)
            cursor.close()
            return True
        except Exception as e:
            print(e)
            return False


if __name__ == "__main__":
    db = MysqlDB(dbname="funddb")
    conn = db.getConn()
    cursor = conn.cursor()
    sql = "select fund_type from fund_list"
    try:
        cursor.execute(sql)
    except Exception as e:
        print(e)

    results = cursor.fetchall()
    fund_type_names = []
    for result in results:
        if result[0] not in fund_type_names:
            fund_type_names.append(result[0])

    sql = "insert into fund_type_table(fund_type_name) values(%s)"

    try:
        cursor.executemany(sql, fund_type_names)
        conn.commit()
    except Exception as e:
        print(e)
        conn.rollback()

    cursor.close()
    conn.close()

    print(fund_type_names)
