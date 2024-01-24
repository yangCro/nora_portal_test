import fire
import json
import urllib.request
from loguru import logger

from .utils import import_data_to_mongodb


ALEX_API_WITH_FILTER = "https://api.openalex.org/works?filter="


def extract_openalex_works(
    start_date="2011-01-01", country_code="DK", sample=1000, seed=111, per_page=50
):
    """
    - params:
        - start_date: str, works pulication start date
        - country_code: str, works that have affliated institutions with given country code
        - sample: int, must be between 1-10000, as rules by openalex
        - seed: int, random seed for paging. Sampling must have a seed to be able to get works
            on different pages.
        - per_page: int, number of works per page
    - return:
        - works: list, list of works extracted from openalex
    """
    assert 0 < sample <= 10000, f"out-of-range sample size: {sample}"
    api_cmd = (
        f"{ALEX_API_WITH_FILTER}from_publication_date:{start_date},"
        f"institutions.country_code:{country_code}&sample={sample}&seed={seed}&per-page={per_page}"
    )
    works = []
    works_got = 0
    page_num = 1
    while works_got < sample:
        url_res = urllib.request.urlopen(api_cmd + f"&page={page_num}")
        results = url_res.read()
        page_res = json.loads(results.decode("utf-8"))
        logger.info(page_res["meta"])
        works += page_res["results"]
        works_got += per_page
        page_num += 1
    logger.info(f"Got {len(works)} works in total")
    return works


def test():
    works = extract_openalex_works()
    count = 0
    for work in works:
        for k, v in work.items():
            print(k, v)
        if count > 2:
            break
        count += 1
    return


def run(mongo_ip="localhost", port=27017, db_name="noradb", collection_name="openalex"):
    works = extract_openalex_works()
    import_data_to_mongodb(works, mongo_ip, port, db_name, collection_name)


if __name__ == "__main__":
    fire.Fire()
