from time import sleep
import requests

import datetime
from util.mysql_util import tableExists, dropTable, createFundHistoryTable
import util.global_variables_util as gvutil
import json

from util.thread_util import runThreads


class FundHistorySpider:
    def __init__(self, conn, fund_codes):
        self.__conn = conn
        self.__fund_codes = fund_codes

    def crawlAndSave(self, start_index=0, end_index=0, page_size=5000):
        """
        爬取所有基金历史数据
        :param start_index: 爬取开始索引
        :param end_index: 爬取结束索引
        :param page_size: 爬取单页面大小
        :return:
        """
        fund_codes_size = len(self.__fund_codes)
        if end_index == 0:
            end_index == fund_codes_size
        for i in range(start_index, end_index):
            page_index = 1
            result = crawlFundHistoryByPage(self.__fund_codes[i], page_index, page_size)
            if result == 0:
                self.__insertIntoFundNoHistoryTable(self.__fund_codes[i])
            else:
                all_history_data_tuple = ()
                while result != 1:
                    one_page_history_data_tuple, is_net_asset_value = result
                    all_history_data_tuple = (*one_page_history_data_tuple, *all_history_data_tuple)
                    page_index += 1
                    result = crawlFundHistoryByPage(self.__fund_codes[i], page_index, page_size)
                if tableExists(self.__conn, "history_" + self.__fund_codes[i] + "_table"):
                    dropTable(self.__conn, "history_" + self.__fund_codes[i] + "_table")
                createFundHistoryTable(self.__conn, self.__fund_codes[i], is_net_asset_value)
                self.__saveFundHistory(self.__fund_codes[i], all_history_data_tuple, is_net_asset_value)
            gvutil.setCrawlCounts(gvutil.getCrawlCounts() + 1)
            print("%s / %s" % (gvutil.getCrawlCounts(), fund_codes_size))
        self.__conn.close()

    def __saveFundHistory(self, fund_code, all_history_data_tuple, is_net_asset_value):
        cursor = self.__conn.cursor()
        if is_net_asset_value:
            sql = "insert into " \
                  "history_" + fund_code + "_table" \
                                           "(date_timestamp, date_string, net_asset_value, accumulated_net_asset_value) " \
                                           "values(%s, %s, %s, %s)"
        else:
            sql = "insert into " \
                  "history_" + fund_code + "_table" \
                                           "(date_timestamp, date_string, earnings_per_10000, 7_day_annual_return) " \
                                           "values(%s, %s, %s, %s)"
        try:
            cursor.executemany(sql, all_history_data_tuple)
            self.__conn.commit()
        except Exception as e:
            print(e)
            self.__conn.rollback()
        cursor.close()

    def __insertIntoFundNoHistoryTable(self, fund_code):
        cursor = self.__conn.cursor()
        sql = "select * from fund_no_history_table where fund_code='" + fund_code + "'"
        cursor.execute(sql)
        count = cursor.rowcount
        if count == 0:
            # fund_no_history_table无该基金记录时再插入，有该基金记录时无操作
            sql = "insert into fund_no_history_table(fund_code) values(%s)"
            val = (fund_code,)
            try:
                cursor.execute(sql, val)
                self.__conn.commit()
            except Exception as e:
                print(e)
                self.__conn.rollback()
        cursor.close()


def crawlFundHistoryByPage(fund_code, page_index=1, page_size=100):
    """
    按页码爬取单个基金历史数据
    :param fund_code: 基金代码
    :param page_index: 爬取页码
    :param page_size: 爬取单页数据大小
    :return:
        0: 该基金无历史数据
        1: 该基金当前页码无数据
        (history_data_list, is_net_asset_value): (历史数据元组, 表头是否为净值)
    """
    connect_counts = 0
    while True:
        try:
            url = "http://api.fund.eastmoney.com/f10/lsjz?callback=jQuery183019601346852042933_1596811572354" \
                  "&fundCode=" + fund_code + \
                  "&pageIndex=" + str(page_index) + \
                  "&pageSize=" + str(page_size) + \
                  "&startDate=&endDate=&_=1596811580612"
            user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) ' \
                         'Chrome/84.0.4147.105 Safari/537.36 '
            referer = "http://fundf10.eastmoney.com/jjjz_" + fund_code + ".html"
            headers = {'User-Agent': user_agent, 'Referer': referer}
            r = requests.get(url, headers=headers)
            break
        except Exception as e:
            connect_counts += 1
            print(e)
            print("trying to connect ... ", connect_counts, "time(s)")
            sleep(5)
    history_data_str = r.text
    history_data_str = history_data_str.replace("jQuery183019601346852042933_1596811572354(", "")
    history_data_str = history_data_str.replace(")", "")
    history_data_json = json.loads(history_data_str)
    total_count = int(history_data_json["TotalCount"])

    if total_count == 0:
        return 0  # 该基金无历史数据
    else:
        if len(history_data_json["Data"]["LSJZList"]) == 0:
            return 1  # 该基金当前页无数据
        else:
            if history_data_json["Data"]["SYType"] is None:
                # 表头为单位净值、累计净值
                is_net_asset_value = True
            else:
                # 表头为每万份收益、7日年化收益率
                is_net_asset_value = False
            history_data_list = []
            for data_item in history_data_json["Data"]["LSJZList"]:
                temp = {"date_string": data_item["FSRQ"],
                        "date_timestamp": datetime.datetime.strptime(data_item["FSRQ"], '%Y-%m-%d').timestamp()}
                if data_item["DWJZ"] == "":
                    temp["net_asset_value_OR_earnings_per_10000"] = None
                else:
                    temp["net_asset_value_OR_earnings_per_10000"] = float(data_item["DWJZ"])

                if data_item["LJJZ"] == "":
                    temp["accumulated_net_asset_value_or_7_day_annual_return"] = None
                else:
                    temp["accumulated_net_asset_value_or_7_day_annual_return"] = float(data_item["LJJZ"])

                history_data_list.insert(0, (
                    temp["date_timestamp"], temp["date_string"], temp["net_asset_value_OR_earnings_per_10000"],
                    temp["accumulated_net_asset_value_or_7_day_annual_return"]))
            history_data_tuple = tuple(history_data_list)
            return history_data_tuple, is_net_asset_value


if __name__ == "__main__":
    runThreads(FundHistorySpider, 10)
