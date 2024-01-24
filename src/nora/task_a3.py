import fire
import json
import schedule
import urllib.request
from datetime import date, timedelta
from loguru import logger

from .utils import update_data_to_mongodb

ALEX_API_WITH_FILTER = "https://api.openalex.org/works?filter="


def extract_openalex_works_given_date_range(
    start_date="2023-01-01", end_date="2023-01-31", country_code="DK", per_page=50
):
    """
    - params:
        - start_date: str, start date for searching work publication date
        - end_date: str, end date for searching work publication date
        - country_code: str, works that have affliated institutions with given country code
        - per_page: int, number of works per page
    - return:
        - works: list, list of works extracted from openalex
    """
    api_cmd = (
        f"{ALEX_API_WITH_FILTER}from_publication_date:{start_date},to_publication_date:{end_date},"
        f"institutions.country_code:{country_code}&per-page={per_page}"
    )

    # check how many works in total
    url_res = urllib.request.urlopen(api_cmd)
    results = url_res.read()
    page_res = json.loads(results.decode("utf-8"))
    num_works = page_res["meta"]["count"]

    works = []
    works_got = 0
    page_num = 1
    while works_got < num_works:
        url_res = urllib.request.urlopen(api_cmd + f"&page={page_num}")
        results = url_res.read()
        page_res = json.loads(results.decode("utf-8"))
        logger.info(page_res["meta"])
        works += page_res["results"]
        works_got += per_page
        page_num += 1
    logger.info(f"Got {len(works)} works in total")
    return works


def get_prev_month_first_last_date():
    """
    - returns:
        - prev_first/last_day_str: str, YYYY-MM-DD for first and last date of previous month
    """
    today = date.today()
    cur_first_day = today.replace(day=1)
    prev_last_day = cur_first_day - timedelta(days=1)
    prev_last_day_str = prev_last_day.strftime("%Y-%m-%d")
    prev_first_day = prev_last_day.replace(day=1)
    prev_first_day_str = prev_first_day.strftime("%Y-%m-%d")
    return prev_first_day_str, prev_last_day_str


def job(mongo_ip="localhost", port=27017, db_name="noradb", collection_name="openalex"):
    # only runs job at the start of each month
    cur_date = date.today()
    if cur_date.day != 1:
        logger.info(
            f"not first day of month: {cur_date.strftime('%Y-%m-%d')}, skip job.."
        )
        return

    # get last month's start date and end date
    prev_first_date, prev_last_date = get_prev_month_first_last_date()

    logger.info(f"getting works from {prev_first_date} to {prev_last_date}")
    works = extract_openalex_works_given_date_range(
        start_date=prev_first_date, end_date=prev_last_date
    )
    update_data_to_mongodb(works, mongo_ip, port, db_name, collection_name)


def run():
    schedule.every().day.at("1:00").do(job)


if __name__ == "__main__":
    fire.Fire()
