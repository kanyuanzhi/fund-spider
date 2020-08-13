import urllib.request as urllib2
import pymongo

# from multiprocessing.pool import ThreadPool




url = 'http://fund.eastmoney.com/js/fundcode_search.js'
user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36'
referer = 'http://fund.eastmoney.com/allfund.html'
headers = {'User-Agent': user_agent, 'Referer': referer}

request = urllib2.Request(url)
request.headers = headers
response = urllib2.urlopen(request)
data_bytes = response.read()
data_str = str(data_bytes, encoding='UTF-8-sig')

# with open('data.txt','w') as f:
#     f.write(data)


data_str = data_str.replace('var r = [[','')
data_str = data_str.replace(']];', '')
data_str = data_str.replace('\"','')

data_list = data_str.split('],[')

print(len(data_list))

myclient = pymongo.MongoClient("mongodb://129.211.145.88:27017/")
mydb = myclient["stockdb"]
collection_names = mydb.list_collection_names()
print(collection_names)

if "fund_list" in collection_names:
    try:
        mycol = mydb["fund_list"]
        mycol.delete_many({})
    except Exception as e:
        print(e)



# mycol = mydb["fund_info"]
#
# mydict = {"name": "Google", "alexa": "1", "url": "https://www.google.com"}
#
# x = mycol.insert_one(mydict)
#
# print(x.inserted_id)



mycol = mydb["fund_list"]

data_dict = []
item = {}

for i in range(len(data_list)):
    item = {}
    temp = data_list[i].split(',')
    item['fund_code'] = temp[0]
    item['fund_full_name'] = ''
    item['fund_short_name'] = temp[2]
    item['fund_type'] = temp[3]
    data_dict.append(item)

try:
    mycol.insert_many(data_dict)
except Exception as e:
    print(e)

url_fund = "http://fundf10.eastmoney.com/jbgk_000006.html"
referer = "http://fundf10.eastmoney.com/000001.html"
headers = {'User-Agent': user_agent, 'Referer': referer}

request = urllib2.Request(url)
request.headers = headers
response = urllib2.urlopen(request)

print(response.read())



