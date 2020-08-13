import urllib.request as urllib2
from time import sleep

from bs4 import BeautifulSoup
import datetime
import pymongo
import re
import json

PAGE_SIZE = 5000


def crawlFundList():
    print("begin crawlFundList")
    url = 'http://fund.eastmoney.com/js/fundcode_search.js'
    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36'
    referer = 'http://fund.eastmoney.com/allfund.html'
    headers = {'User-Agent': user_agent, 'Referer': referer}

    request = urllib2.Request(url)
    request.headers = headers
    response = urllib2.urlopen(request)
    data_bytes = response.read()
    data_str = str(data_bytes, encoding='UTF-8-sig')

    data_str = data_str.replace('var r = [[', '')
    data_str = data_str.replace(']];', '')
    data_str = data_str.replace('\"', '')

    data_list = data_str.split('],[')

    data_dict = []
    for i in range(len(data_list)):
        item = {}
        temp = data_list[i].split(',')
        item['fund_code'] = temp[0]
        item['fund_short_name'] = temp[2]
        item['fund_type'] = temp[3]
        data_dict.append(item)

    print("finish crawlFundList")
    return data_dict


def saveFundList(data_dict):
    print("begin saveFundList")
    myclient = pymongo.MongoClient("mongodb://129.211.145.88:27017/")
    mydb = myclient["funddb"]
    try:
        result = mydb.fund_list.insert_many(data_dict)
    except Exception as e:
        print(e)
        return

    print("finish saveFundList")
    return result


def crawlFundInfo(fund_code):
    url_fund = "http://fundf10.eastmoney.com/jbgk_" + fund_code + ".html"
    referer = "http://fundf10.eastmoney.com/000001.html"
    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36'

    headers = {'User-Agent': user_agent, 'Referer': referer}

    request = urllib2.Request(url_fund)
    request.headers = headers

    # response = urllib2.urlopen(request)
    try:
        response = urllib2.urlopen(request)
    except Exception as e:
        print(e)
        return False

    html = response.read()
    soup = BeautifulSoup(html, 'lxml')
    data_dict = {}
    table_rows = soup.find("table", attrs={"class", "info"}).contents
    # print(table_rows)
    data_dict["fund_code"] = fund_code
    data_dict["fund_full_name"] = table_rows[0].contents[1].string
    data_dict["fund_short_name"] = table_rows[0].contents[3].string
    data_dict["fund_type"] = table_rows[1].contents[3].string
    if table_rows[2].contents[1].string is None:
        data_dict["fund_issue_date"] = None
        data_dict["fund_issue_date_string"] = None
    else:
        data_dict["fund_issue_date"] = datetime.datetime.strptime(table_rows[2].contents[1].string,
                                                                  '%Y年%m月%d日').timestamp()
        data_dict["fund_issue_date_string"] = table_rows[2].contents[1].string

    if table_rows[2].contents[3].string.split(" / ")[0] == '':
        data_dict["fund_launch_date"] = None
        data_dict["fund_launch_date_string"] = None
    else:
        data_dict["fund_launch_date"] = datetime.datetime.strptime(table_rows[2].contents[3].string.split(" / ")[0],
                                                                   '%Y年%m月%d日').timestamp()
        data_dict["fund_launch_date_string"] = table_rows[2].contents[3].string.split(" / ")[0]

    if table_rows[3].contents[1].string == "---":
        data_dict["fund_asset_size"] = None
    else:
        asset_size = table_rows[3].contents[1].string.split("亿元")[0].replace(',', "")
        data_dict["fund_asset_size"] = float(asset_size)

    data_dict["fund_company"] = table_rows[4].contents[1].string
    data_dict["fund_trustee"] = table_rows[4].contents[3].string

    a_manager = table_rows[5].contents[1].find_all("a")
    if len(a_manager) < 2:
        data_dict["fund_manager"] = table_rows[5].contents[1].string
    else:
        fund_manager = a_manager[0].string
        for i in range(1, len(a_manager)):
            fund_manager += "," + a_manager[i].string
        data_dict["fund_manager"] = fund_manager

    dividend_info = table_rows[5].contents[3].string
    dividend_number = re.findall("\d+\.?\d*", dividend_info)
    # print(dividend_number)
    data_dict["fund_dividend_payment_per_unit"] = float(dividend_number[0])
    data_dict["fund_dividend_amounts"] = int(dividend_number[1])

    if soup.find(attrs={"class", "bs_jz"}) is None:
        data_dict["fund_purchase_status"] = None
        data_dict["fund_redemption_status"] = None
    else:
        # print(soup.find(attrs={"class", "bs_jz"}).find(attrs={"class", "col-right"}))
        if soup.find(attrs={"class", "bs_jz"}).find(attrs={"class", "col-right"}) is None:
            data_dict["fund_purchase_status"] = None
            data_dict["fund_redemption_status"] = None
        else:
            status_list = soup.find(attrs={"class", "bs_jz"}).find(attrs={"class", "col-right"}).find_all("p")[
                1].find_all("span")
            # print(status_list)
            if status_list[2].string is None:
                data_dict["fund_purchase_status"] = status_list[0].string.strip()
                data_dict["fund_redemption_status"] = status_list[0].string.strip()
            else:
                if len(status_list) == 4:
                    if status_list[3].string is None:
                        data_dict["fund_purchase_status"] = status_list[0].string.strip() + "(" + status_list[
                            2].string.strip() + ")"
                        data_dict["fund_redemption_status"] = status_list[0].string.strip()
                    else:
                        data_dict["fund_purchase_status"] = status_list[0].string.strip() + "(" + status_list[
                            2].string.strip() + ")"
                        data_dict["fund_redemption_status"] = status_list[3].string.strip()
                else:
                    data_dict["fund_purchase_status"] = status_list[0].string.strip()
                    data_dict["fund_redemption_status"] = status_list[2].string.strip()
    return data_dict


def saveFundInfo(fund_codes, begin_index=0):
    myclient = pymongo.MongoClient("mongodb://129.211.145.88:27017/")
    mydb = myclient["funddb"]
    for i in range(begin_index, len(fund_codes)):
        fund_info_dict = crawlFundInfo(fund_codes[i])
        if fund_info_dict is False:
            false_counts = 1
            while fund_info_dict is False:
                print(false_counts)
                sleep(2)
                fund_info_dict = crawlFundInfo(fund_codes[i])
                false_counts += 1
        try:
            result = mydb.fund_info.insert_one(fund_info_dict)
        except Exception as e:
            print(e)
            return
        print("finish ", i, "/", len(fund_codes))
    return


def crawlFundNetValue(fund_code):
    index = 1
    data_json = crawlFundNetValueByIndex(fund_code, index)
    if data_json is False:
        false_counts = 1
        while data_json is False:
            print(false_counts)
            sleep(2)
            data_json = crawlFundNetValueByIndex(fund_code, index)
            false_counts += 1

    if data_json == 0:
        return False
    else:
        data_dict = []
        while data_json != 1:
            for data_item in data_json["Data"]["LSJZList"]:
                temp = {}
                temp["date_string"] = data_item["FSRQ"]
                temp["date"] = datetime.datetime.strptime(data_item["FSRQ"], '%Y-%m-%d').timestamp()
                if data_item["DWJZ"] == "":
                    temp["net_asset_value"] = None
                else:
                    temp["net_asset_value"] = float(data_item["DWJZ"])

                if data_item["LJJZ"] == "":
                    temp["accumulated_net_asset_value"] = None
                else:
                    temp["accumulated_net_asset_value"] = float(data_item["LJJZ"])

                data_dict.append(temp)
            index += 1
            data_json = crawlFundNetValueByIndex(fund_code, index)
            if data_json is False:
                false_counts = 1
                while data_json is False:
                    print(false_counts)
                    sleep(2)
                    data_json = crawlFundNetValueByIndex(fund_code, index)
                    false_counts += 1
            # sleep(0.2)

    # print(data_dict)
    return data_dict


def crawlFundNetValueByIndex(fund_code, index):
    # print(fund_code)
    url = "http://api.fund.eastmoney.com/f10/lsjz?callback=jQuery183019601346852042933_1596811572354&fundCode=" + fund_code + "&pageIndex=" + str(
        index) + "&pageSize=" + str(globals()["PAGE_SIZE"]) + "&startDate=&endDate=&_=1596811580612"
    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36'
    referer = "http://fundf10.eastmoney.com/jjjz_" + fund_code + ".html"
    headers = {'User-Agent': user_agent, 'Referer': referer}

    request = urllib2.Request(url)
    request.headers = headers
    try:
        response = urllib2.urlopen(request)
    except Exception as e:
        print(e)
        return False
        # sleep(0.5)
        # response = urllib2.urlopen(request)
    data_bytes = response.read()
    data_str = str(data_bytes, encoding='UTF-8-sig')
    data_str = data_str.replace("jQuery183019601346852042933_1596811572354(", "")
    data_str = data_str.replace(")", "")
    data_json = json.loads(data_str)

    total_count = int(data_json["TotalCount"])

    if total_count == 0:
        return 0  # 该基金无历史数据
    else:
        if len(data_json["Data"]["LSJZList"]) == 0:
            return 1  # 该基金当前页码无数据
        else:
            return data_json


def saveFundNetValue(fund_codes, begin_index=0):
    myclient = pymongo.MongoClient("mongodb://129.211.145.88:27017/")
    mydb = myclient["funddb"]
    # delCol(mydb, "fund_no_data_list")
    collection_names = mydb.list_collection_names()
    for i in range(begin_index, len(fund_codes)):
        if "history_" + fund_codes[i] in collection_names:
            delCol(mydb, "history_" + fund_codes[i])
        fund_net_value_dict = crawlFundNetValue(fund_codes[i])
        if not fund_net_value_dict:
            # 基金无历史数据
            if mydb["fund_no_data_list"].count_documents({"fund_code": fund_codes[i]}) == 0:
                mydb["fund_no_data_list"].insert_one({"fund_code": fund_codes[i]})
            else:
                continue
        else:
            try:
                mydb["history_" + fund_codes[i]].insert_many(fund_net_value_dict)
            except Exception as e:
                print(e)
                return
            print("finish ", i, "/", len(fund_codes))
    return


def delCol(db, col):
    try:
        db[col].delete_many({})
    except Exception as e:
        print(e)


if __name__ == "__main__":
    myclient = pymongo.MongoClient("mongodb://129.211.145.88:27017/")
    mydb = myclient["funddb"]
    collection_names = mydb.list_collection_names()

    fund_list_dict = crawlFundList()

    if "fund_list" in collection_names:
        delCol(mydb, "fund_list")
    saveFundList(fund_list_dict)

    fund_codes = []
    for item in fund_list_dict:
        fund_codes.append(item["fund_code"])

    if "fund_info" in collection_names:
        delCol(mydb, "fund_info")
    saveFundInfo(fund_codes)

    # print(fund_codes[2522])
    # crawlFundInfo(fund_codes[3877])

    # saveFundNetValue(fund_codes, 10676)
    # crawlFundNetValue("000002")
