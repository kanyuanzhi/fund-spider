import requests
from time import sleep
from util.mysql_util import MysqlDB, tableExists, truncateTable, createFundListTable


def crawlFundList():
    url = 'http://fund.eastmoney.com/js/fundcode_search.js'
    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) ' \
                 'Chrome/84.0.4147.105 Safari/537.36 '
    referer = 'http://fund.eastmoney.com/allfund.html'
    headers = {'User-Agent': user_agent, 'Referer': referer}
    connect_counts = 0
    while True:
        try:
            r = requests.get(url, headers=headers)
            break
        except Exception as e:
            connect_counts += 1
            print(e)
            print("trying to connect ... ", connect_counts, "time(s)")
            sleep(5)
    response_str = r.text
    response_str = response_str.replace('var r = [[', '')
    response_str = response_str.replace(']];', '')
    response_str = response_str.replace('\"', '')
    response_array = response_str.split('],[')

    fund_list_array = []
    for i in range(len(response_array)):
        temp = response_array[i].split(",")
        fund_list_array.append((temp[0], temp[2], temp[3]))
    fund_list_tuple = tuple(fund_list_array)
    return fund_list_tuple


def getFundCodes():
    fund_codes = []
    fund_list_tuple = crawlFundList()
    for i in range(len(fund_list_tuple)):
        fund_codes.append(fund_list_tuple[i][0])
    return fund_codes


class FundListSpider:
    def __init__(self, conn):
        self.__conn = conn

    def crawlAndSave(self):
        fund_list_tuple = crawlFundList()
        if tableExists(self.__conn, "fund_list_table"):
            truncateTable(self.__conn, "fund_list_table")
        else:
            createFundListTable(self.__conn)
        self.__saveFundList(fund_list_tuple)
        self.__conn.close()

    def __saveFundList(self, fund_list_tuple):
        cursor = self.__conn.cursor()
        sql = "insert into fund_list_table(fund_code, fund_short_name, fund_type) values(%s, %s, %s);"
        try:
            cursor.executemany(sql, fund_list_tuple)
            conn.commit()
        except Exception as e:
            print(e)
            conn.rollback()
        cursor.close()


if __name__ == "__main__":
    # response_tuple = crawlFundList()
    # saveFundList(response_tuple)
    # print(getFundCodes())

    db = MysqlDB()
    conn = db.getConnFromPool()
    fls = FundListSpider(conn)
    fls.crawlAndSave()
