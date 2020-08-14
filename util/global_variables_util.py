class GlobalVariables:
    updated_number = 0
    no_updated_number = 0
    crawl_counts = 0


def getUpdatedNumber():
    return GlobalVariables.updated_number


def setUpdatedNumber(num):
    GlobalVariables.updated_number = num


def getNoUpdatedNumber():
    return GlobalVariables.no_updated_number


def setNoUpdatedNumber(num):
    GlobalVariables.no_updated_number = num


def getCrawlCounts():
    return GlobalVariables.crawl_counts


def setCrawlCounts(count):
    GlobalVariables.crawl_counts = count
