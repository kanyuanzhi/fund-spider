from util.thread_util import runThreads
from crawl_fund_latest import FundLatestSpider
from apscheduler.schedulers.blocking import BackgroundScheduler


def job():
    runThreads(FundLatestSpider, 10)


scheduler = BackgroundScheduler()
scheduler.add_job(job, 'cron', day_of_week='1-7', hour=6, minute=30)
scheduler.start()
