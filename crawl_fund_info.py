import requests
from time import sleep

from bs4 import BeautifulSoup
import datetime
import re

from util.thread_util import runThreads

spider_counts = 0


class FundInfoSpider:
    def __init__(self, conn, fund_codes):
        self.__conn = conn
        self.__fund_codes = fund_codes

    def crawlAndSave(self, start_index=0, end_index=0):
        fund_codes_size = len(self.__fund_codes)
        if end_index == 0:
            end_index = fund_codes_size
        for i in range(start_index, end_index):
            fund_info_tuple = crawlFundInfo(self.__fund_codes[i])
            self.__saveFundInfo(fund_info_tuple)
            global spider_counts
            spider_counts += 1
            print("%s / %s" % (spider_counts, fund_codes_size))
        self.__conn.close()

    def __saveFundInfo(self, fund_info_tuple):
        cursor = self.__conn.cursor()
        sql = "insert into fund_info_table(fund_code, fund_full_name, fund_short_name, fund_type," \
              "fund_issue_date, fund_issue_date_string, fund_launch_date, fund_launch_date_string, fund_asset_size, " \
              "fund_company, fund_trustee, fund_manager, fund_dividend_payment_per_unit, fund_dividend_amounts, " \
              "fund_purchase_status, fund_redemption_status) " \
              "values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
        try:
            cursor.execute(sql, fund_info_tuple)
            self.__conn.commit()
        except Exception as e:
            print(e)
            self.__conn.rollback()
        cursor.close()


def crawlFundInfo(fund_code):
    url = "http://fundf10.eastmoney.com/jbgk_" + fund_code + ".html"
    referer = "http://fundf10.eastmoney.com/000001.html"
    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) ' \
                 'Chrome/84.0.4147.105 Safari/537.36 '
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
    html = r.text
    soup = BeautifulSoup(html, 'lxml')
    response_dict = {}
    table_rows = soup.find("table", attrs={"class", "info"}).contents
    # print(table_rows)
    response_dict["fund_code"] = fund_code
    response_dict["fund_full_name"] = table_rows[0].contents[1].string
    response_dict["fund_short_name"] = table_rows[0].contents[3].string
    response_dict["fund_type"] = table_rows[1].contents[3].string
    if table_rows[2].contents[1].string is None:
        response_dict["fund_issue_date"] = None
        response_dict["fund_issue_date_string"] = None
    else:
        response_dict["fund_issue_date"] = datetime.datetime.strptime(table_rows[2].contents[1].string,
                                                                      '%Y年%m月%d日').timestamp()
        response_dict["fund_issue_date_string"] = datetime.datetime.strptime(table_rows[2].contents[1].string,
                                                                             '%Y年%m月%d日').strftime("%Y-%m-%d")

    if table_rows[2].contents[3].string.split(" / ")[0] == '':
        response_dict["fund_launch_date"] = None
        response_dict["fund_launch_date_string"] = None
    else:
        response_dict["fund_launch_date"] = datetime.datetime.strptime(table_rows[2].contents[3].string.split(" / ")[0],
                                                                       '%Y年%m月%d日').timestamp()
        response_dict["fund_launch_date_string"] = datetime.datetime.strptime(
            table_rows[2].contents[3].string.split(" / ")[0],
            '%Y年%m月%d日').strftime("%Y-%m-%d")

    if table_rows[3].contents[1].string == "---":
        response_dict["fund_asset_size"] = None
    else:
        asset_size = table_rows[3].contents[1].string.split("亿元")[0].replace(',', "")
        response_dict["fund_asset_size"] = float(asset_size)

    response_dict["fund_company"] = table_rows[4].contents[1].string
    response_dict["fund_trustee"] = table_rows[4].contents[3].string

    a_manager = table_rows[5].contents[1].find_all("a")
    if len(a_manager) < 2:
        response_dict["fund_manager"] = table_rows[5].contents[1].string
    else:
        fund_manager = a_manager[0].string
        for i in range(1, len(a_manager)):
            fund_manager += "," + a_manager[i].string
        response_dict["fund_manager"] = fund_manager

    dividend_info = table_rows[5].contents[3].string
    dividend_number = re.findall("\d+\.?\d*", dividend_info)
    # print(dividend_number)
    response_dict["fund_dividend_payment_per_unit"] = float(dividend_number[0])
    response_dict["fund_dividend_amounts"] = int(dividend_number[1])

    if soup.find(attrs={"class", "bs_jz"}) is None:
        response_dict["fund_purchase_status"] = None
        response_dict["fund_redemption_status"] = None
    else:
        # print(soup.find(attrs={"class", "bs_jz"}).find(attrs={"class", "col-right"}))
        if soup.find(attrs={"class", "bs_jz"}).find(attrs={"class", "col-right"}) is None:
            response_dict["fund_purchase_status"] = None
            response_dict["fund_redemption_status"] = None
        else:
            status_list = soup.find(attrs={"class", "bs_jz"}).find(attrs={"class", "col-right"}).find_all("p")[
                1].find_all("span")
            # print(status_list)
            if status_list[2].string is None:
                response_dict["fund_purchase_status"] = status_list[0].string.strip()
                response_dict["fund_redemption_status"] = status_list[0].string.strip()
            else:
                if len(status_list) == 4:
                    if status_list[3].string is None:
                        response_dict["fund_purchase_status"] = status_list[0].string.strip() + "(" + status_list[
                            2].string.strip() + ")"
                        response_dict["fund_redemption_status"] = status_list[0].string.strip()
                    else:
                        response_dict["fund_purchase_status"] = status_list[0].string.strip() + "(" + status_list[
                            2].string.strip() + ")"
                        response_dict["fund_redemption_status"] = status_list[3].string.strip()
                else:
                    response_dict["fund_purchase_status"] = status_list[0].string.strip()
                    response_dict["fund_redemption_status"] = status_list[2].string.strip()
    response_tuple = (response_dict["fund_code"], response_dict["fund_full_name"], response_dict["fund_short_name"],
                      response_dict["fund_type"], response_dict["fund_issue_date"],
                      response_dict["fund_issue_date_string"],
                      response_dict["fund_launch_date"], response_dict["fund_launch_date_string"],
                      response_dict["fund_asset_size"], response_dict["fund_company"], response_dict["fund_trustee"],
                      response_dict["fund_manager"], response_dict["fund_dividend_payment_per_unit"],
                      response_dict["fund_dividend_amounts"],
                      response_dict["fund_purchase_status"], response_dict["fund_redemption_status"])
    return response_tuple


if __name__ == "__main__":
    runThreads(FundInfoSpider, 10)
