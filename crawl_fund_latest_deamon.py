from util.thread_util import runThreads
from util.mysql_util import saveToCrawlFundLatestLogTable, MysqlDB
import util.global_variables_util as gvutil
from crawl_fund_latest import FundLatestSpider
import crawl_fund_latest
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler
import datetime

db = MysqlDB()


def job():
    conn = db.getConnFromPool()
    runThreads(FundLatestSpider, 10)
    print(gvutil.getUpdatedNumber(), gvutil.getNoUpdatedNumber())
    datetime_timestamp = datetime.datetime.now().timestamp()
    datetime_string = datetime.datetime.fromtimestamp(datetime_timestamp)
    # date_time = datetime.datetime.now()
    # datetime_string = datetime.datetime.strftime(datetime_timestamp, "%Y-%m-%d %H:%M:%S")
    saveToCrawlFundLatestLogTable(conn, (
        gvutil.getUpdatedNumber(), gvutil.getNoUpdatedNumber(), datetime_timestamp, datetime_string))
    conn.close()


if __name__ == "__main__":
    # scheduler = BlockingScheduler()
    # scheduler.add_job(job, 'cron', day_of_week='1-5', hour=19, minute=00)
    # scheduler.add_job(job, 'cron', day_of_week='1-5', hour=22, minute=00)
    # scheduler.start()
    job()
