from util.thread_util import runThreads
from util.mysql_util import existsInFundNoHistoryTable, removeFromFundNoHistoryTable, createFundHistoryTable

from crawl_fund_history import crawlFundHistoryByPage

spider_counts = 0


class FundLatestSpider:
    def __init__(self, conn, fund_codes):
        self.__conn = conn
        self.__fund_codes = fund_codes

    def crawlAndSave(self, start_index=0, end_index=0, page_size=10):
        fund_codes_size = len(self.__fund_codes)
        if end_index == 0:
            end_index == fund_codes_size
        for i in range(start_index, end_index):
            result = crawlFundHistoryByPage(self.__fund_codes[i], 1, page_size)
            if result != 0:
                candidate_history_data_tuple, is_net_asset_value = result
                self.__updateFundHistory(self.__fund_codes[i], candidate_history_data_tuple, is_net_asset_value)
            global spider_counts
            spider_counts += 1
            print("%s / %s" % (spider_counts, fund_codes_size))
        self.__conn.close()

    def __updateFundHistory(self, fund_code, candidate_history_data_tuple, is_net_asset_value):
        exists_in_fund_no_history = existsInFundNoHistoryTable(self.__conn, fund_code)
        if exists_in_fund_no_history:
            removeFromFundNoHistoryTable(self.__conn, fund_code)
            createFundHistoryTable(self.__conn, fund_code, is_net_asset_value)
            eligible_history_data_tuple = candidate_history_data_tuple
        else:
            latest_date_in_history = self.__getLatestDate(fund_code)
            eligible_history_data_list = []
            for data in candidate_history_data_tuple:
                if data[0] > latest_date_in_history:
                    eligible_history_data_list.append(data)
            eligible_history_data_tuple = tuple(eligible_history_data_list)
        if len(eligible_history_data_tuple) == 0:
            print(fund_code, "no need to be updated")
        else:
            print("here")
            self.__saveFundLatest(fund_code, eligible_history_data_tuple, is_net_asset_value)
            print(fund_code, "has updated", len(eligible_history_data_tuple), "record(s)")

    def __saveFundLatest(self, fund_code, eligible_history_data_tuple, is_net_asset_value):
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
            cursor.executemany(sql, eligible_history_data_tuple)
            self.__conn.commit()
        except Exception as e:
            print(e)
            self.__conn.rollback()
        cursor.close()

    def __getLatestDate(self, fund_code):
        cursor = self.__conn.cursor()
        sql = "select date_timestamp from history_" + fund_code + "_table order by id desc limit 1"
        try:
            cursor.execute(sql)
        except Exception as e:
            print(e)
            return
        latest_date = cursor.fetchone()[0]
        cursor.close()
        return latest_date


if __name__ == "__main__":
    runThreads(FundLatestSpider, 10)
