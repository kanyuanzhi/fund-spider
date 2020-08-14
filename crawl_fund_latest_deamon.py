from util.thread_util import runThreads
import util.global_variables_util as gvutil
from crawl_fund_latest import FundLatestSpider
import crawl_fund_latest
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler
import datetime


def job():
    runThreads(FundLatestSpider, 10)
    print(gvutil.getUpdatedNumber(), gvutil.getNoUpdatedNumber())


if __name__ == "__main__":
    # scheduler = BlockingScheduler()
    # scheduler.add_job(job, 'cron', day_of_week='1-5', hour=19, minute=00)
    # scheduler.add_job(job, 'cron', day_of_week='1-5', hour=22, minute=00)
    # scheduler.start()
    job()
