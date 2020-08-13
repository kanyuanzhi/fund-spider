import requests
import datetime
import json
from time import sleep

from old_src.connect_to_db import MysqlDB
from crawl_fund_list import crawlFundList


def crawlAllFundLatestNetValue(db, conn, fund_codes, start_index=0):
    for i in range(start_index, len(fund_codes)):
        exists_in_fund_no_history = db.existsInFundNoHistory(fund_codes[i])
        latest_data = crawFundLatestNetValue(fund_codes[i])
        if latest_data is False:
            # 当前基金无最新值，
            if exists_in_fund_no_history:
                # 当前基金存在于fund_no_history表中，是已经记录的后端基金（并如前端）或新基金，无需操作
                continue
            else:
                # 当前基金不存在于fund_no_history表中，是从基金列表中爬取的新基金且之前未记录，
                # 则在fund_no_history表中记录该基金
                cursor = conn.cursor()
                sql = "insert into fund_no_history(fund_code) values(%s)"
                val = (fund_codes[i],)
                try:
                    cursor.execute(sql, val)
                    cursor.close()
                    conn.commit()
                except Exception as e:
                    print(e)
                    conn.rollback()
        else:
            if exists_in_fund_no_history:
                # 当前基金在fund_no_history表中且有最新值，
                # 则需将该基金代码从fund_no_history表中移除并新建history_fund_code表
                db.removeFromFundNoHistory(fund_codes[i])
                db.createFundHistoryNetValueTable(fund_codes[i])
            # 更新history_fund_code表
            saveFundLatestNetValue(conn, fund_codes[i], latest_data)
        print("update ", i, "/", len(fund_codes))
    conn.close()


def crawFundLatestNetValue(fund_code):
    # 爬取当前基金最新的10条数据
    latest_count = 7
    while True:
        try:
            # r = requests.get("http://fund.eastmoney.com/" + fund_code + ".html")
            url = "http://api.fund.eastmoney.com/f10/lsjz?callback=jQuery183019601346852042933_1596811572354&" \
                  "fundCode=" + fund_code \
                  + "&pageIndex=1&pageSize=" + str(latest_count) + "&startDate=&endDate=&_=1596811580612"
            user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) ' \
                         'Chrome/84.0.4147.105 Safari/537.36 '
            referer = "http://fundf10.eastmoney.com/jjjz_" + fund_code + ".html"
            headers = {'User-Agent': user_agent, 'Referer': referer}
            r = requests.get(url, headers=headers)
            break
        except Exception as e:
            print(e)
            sleep(5)
    latest_data_str = r.text
    # print(r.encoding)
    latest_data_str = latest_data_str.replace("jQuery183019601346852042933_1596811572354(", "")
    latest_data_str = latest_data_str.replace(")", "")
    latest_data_json = json.loads(latest_data_str)
    total_count = int(latest_data_json["TotalCount"])

    if total_count == 0:
        return False
    else:
        if latest_data_json["Data"]["SYType"] is None:
            # 表头为单位净值、累计净值
            table_head = 0
        else:
            # 表头为每万份收益、7日年化收益率
            table_head = 1
        latest_data_list = []
        for data_item in latest_data_json["Data"]["LSJZList"]:
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

            latest_data_list.insert(0, (
                temp["date_timestamp"], temp["date_string"], temp["net_asset_value_OR_earnings_per_10000"],
                temp["accumulated_net_asset_value_or_7_day_annual_return"]))
        return tuple(latest_data_list), table_head

    # doc = pq(r.text.encode("ISO-8859-1").decode("utf8"))
    # try:
    #     latest_date_string = doc(".dataItem02 p").text()
    #     latest_date_string = latest_date_string.replace("单位净值 (", "")
    #     latest_date_string = latest_date_string.replace(")", "")
    #     latest_date_timestamp = datetime.datetime.strptime(latest_date_string, '%Y-%m-%d').timestamp()
    #     latest_net_asset_value = doc(".dataItem02 .dataNums span:first-child").text()
    #     latest_accumulated_net_asset_value = doc(".dataItem03 .dataNums span:first-child").text()
    #     latest_data = (latest_date_timestamp, latest_date_string, float(latest_net_asset_value),
    #                    float(latest_accumulated_net_asset_value))
    # except Exception as e:
    #     print(fund_code)
    #     print(e)
    #     return False


def saveFundLatestNetValue(conn, fund_code, latest_data):
    date_timestamp = latest_data[0]
    if checkLatest(conn, fund_code, date_timestamp):
        cursor = conn.cursor()
        sql = "insert into " \
              "history_" + fund_code + \
              "(date_timestamp, date_string, net_asset_value, accumulated_net_asset_value) " \
              "values(%s, %s, %s, %s)"
        try:
            cursor.execute(sql, latest_data)
            cursor.close()
            conn.commit()
        except Exception as e:
            print(e)
            conn.rollback()
    else:
        print("not latest data!")
        return


def checkLatest(conn, fund_code, date_timestamp):
    cursor = conn.cursor()
    sql = "select * from history_" + fund_code + " where date_timestamp=" + str(date_timestamp)
    try:
        cursor.execute(sql)
    except Exception as e:
        print(e)
    count = cursor.rowcount
    cursor.close()
    if count == 0:
        return True
    else:
        return False


if __name__ == "__main__":
    db = MysqlDB(dbname="funddb")
    conn = db.getConn()

    funds = crawlFundList()
    fund_codes = []
    for fund in funds:
        fund_codes.append(fund[0])
    # if latest_data is False:
    #     pass
    # else:
    #     saveFundLatestNetValue()

    # crawlAllFundLatestNetValue(db, conn, fund_codes, start_index=0)
    latest_data = crawFundLatestNetValue("000013")
    print(latest_data)
