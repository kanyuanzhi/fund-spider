from time import sleep
import requests

import datetime
from old_src.connect_to_db import MysqlDB
from crawl_fund_list import crawlFundList
import json

PAGE_SIZE = 5000


def crawlMain(fund_code):
    index = 1
    result = crawlFundHistoryPage(fund_code, index)

    if result == 0:
        return False
    else:
        all_history_data_tuple = ()
        while result != 1:
            (one_page_history_data_list, is_net_asset_value) = result
            all_history_data_tuple = (*one_page_history_data_list, *all_history_data_tuple)
            index += 1
            result = crawlFundHistoryPage(fund_code, index)

    # print(data_dict)
    history_tuple = tuple(data_list)
    return history_tuple


def crawlFundHistoryPage(fund_code, page_index=1, page_size=5000):
    """
    按页码爬取基金历史数据
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
            return 1  # 该基金当前页码无数据
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


def saveFundNetValue(fund_codes, begin_index=0):
    db = MysqlDB(dbname="funddb")
    conn = db.getConn()
    cursor = conn.cursor()

    if not db.tableExists("fund_no_history"):
        db.createFundNoHistory()

    for i in range(begin_index, len(fund_codes)):
        if db.tableExists("history_" + fund_codes[i]):
            db.dropTable("history_" + fund_codes[i])

        history = crawlFundNetValue(fund_codes[i])
        if not history:
            # 基金无历史数据
            sql = "select * from fund_no_history where fund_code='" + fund_codes[i] + "'"
            cursor.execute(sql)
            count = cursor.rowcount
            if count == 0:
                sql = "insert into fund_no_history(fund_code) values(%s)"
                val = (fund_codes[i],)
                try:
                    cursor.execute(sql, val)
                    conn.commit()
                except Exception as e:
                    print(e)
                    conn.rollback()
            else:
                continue
        else:
            db.createFundHistoryNetValueTable(fund_codes[i])
            sql = "insert into " \
                  "history_" + fund_codes[i] + \
                  "(date_timestamp, date_string, net_asset_value, accumulated_net_asset_value) " \
                  "values(%s, %s, %s, %s)"
            try:
                cursor.executemany(sql, history)
                conn.commit()
            except Exception as e:
                print(e)
                conn.rollback()
            print("finish ", i, "/", len(fund_codes))
    cursor.close()
    conn.close()


if __name__ == "__main__":
    funds = crawlFundList()
    fund_codes = []
    for fund in funds:
        fund_codes.append(fund[0])

    # history = crawlFundNetValue("000002")
    # print(history)
    saveFundNetValue(fund_codes)
