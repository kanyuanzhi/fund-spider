import pymongo

myclient = pymongo.MongoClient("mongodb://admin:woodwolf2@129.211.145.88:27017/")
mydb = myclient["stockdb"]
mydb.authenticate("admin","woodwolf2")

collection_names = mydb.list_collection_names()

for name in collection_names:
    origin = "stockdb" + "." + name
    target = "funddb" + "." + name
