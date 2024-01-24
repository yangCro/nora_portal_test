import fire
import matplotlib.pyplot as plt
from loguru import logger
from pymongo import MongoClient


def task_d(
    mongo_ip="localhost", port=27017, db_name="noradb", collection_name="openalex"
):
    client = MongoClient(mongo_ip, port)
    db = client[db_name]
    collection = db[collection_name]
    pipeline = [
        {"$unwind": "$authorships"},
        {"$match": {"authorships.countries": "DK"}},
        {
            "$group": {
                "_id": "$authorships.raw_author_name",
                "number": {"$sum": 1},
                "affliation": {"$first": "$authorships.raw_affiliation_string"},
            }
        },
        {
            "$sort": {"number": -1},
        },
    ]
    output = collection.aggregate(pipeline)
    author_count = list(output)
    logger.info("Top 20 authors and their occurrences: ")
    for i in range(20):
        logger.info(
            f"#{i+1}: {author_count[i]['_id']}, {author_count[i]['number']}, {author_count[i]['affliation']}"
        )
    client.close()


def task_e(
    mongo_ip="localhost", port=27017, db_name="noradb", collection_name="openalex"
):
    client = MongoClient(mongo_ip, port)
    db = client[db_name]
    collection = db[collection_name]

    # top 10 authors with most citations
    pipeline = [
        {"$unwind": "$authorships"},
        {
            "$group": {
                "_id": "$authorships.raw_author_name",
                "number": {"$sum": "$cited_by_count"},
            }
        },
        {
            "$sort": {"number": -1},
        },
    ]
    output = collection.aggregate(pipeline)
    author_count = list(output)
    logger.info("Top 10 authors with most citations: ")
    for i in range(10):
        logger.info(f"#{i+1}: {author_count[i]['_id']}, {author_count[i]['number']}")

    # average citations per publication
    pipeline = [
        {
            "$group": {
                "_id": "null",
                "mean_citation": {"$avg": "$cited_by_count"},
            }
        },
    ]
    output = collection.aggregate(pipeline)
    mean_citation = round(list(output)[0]["mean_citation"], 2)
    logger.info(f"Average citations per publication is: {mean_citation}")
    client.close()


def task_f(
    mongo_ip="localhost", port=27017, db_name="noradb", collection_name="openalex"
):
    client = MongoClient(mongo_ip, port)
    db = client[db_name]
    collection = db[collection_name]

    # top 10 most cited works in related work
    pipeline = [
        {"$unwind": "$related_works"},
        {
            "$group": {
                "_id": "$related_works",
                "num": {"$sum": 1},
            }
        },
        {
            "$sort": {"num": -1},
        },
    ]
    output = list(collection.aggregate(pipeline))
    logger.info("Top 10 most cited works in related_works: ")
    for i in range(10):
        logger.info(f"#{i+1}: {output[i]['_id']}, {output[i]['num']}")
    client.close()


def task_g(
    mongo_ip="localhost", port=27017, db_name="noradb", collection_name="openalex"
):
    client = MongoClient(mongo_ip, port)
    db = client[db_name]
    collection = db[collection_name]
    # get author numbers per publication
    logger.info("getting num of authors per publication...")
    pipeline = [
        {
            "$addFields": {
                "num_authors": {"$size": "$authorships"},
            }
        },
    ]
    output = list(collection.aggregate(pipeline))
    num_authors = [d["num_authors"] for d in output]
    client.close()

    # plot histogram for number of authors distribution
    logger.info("ploting histogram...")
    plt.hist(num_authors, bins=20)
    plt.xlabel("Num_Authors")
    plt.ylabel("Frequency")
    plt.title("Num of Authors Per Publication")
    plt.show()


if __name__ == "__main__":
    fire.Fire()
