import fire
from loguru import logger
from pymongo import MongoClient


def import_data_to_mongodb(
    data, mongo_ip="localhost", port=27017, db_name="noradb", collection_name="openalex"
):
    client = MongoClient(mongo_ip, port)
    db = client[db_name]
    collection = db[collection_name]
    collection.insert_many(data)
    client.close()
    logger.info(f"loading data into mongodb sucessful, data num: {len(data)}")
    logger.info(
        f"mongodb info. IP: {mongo_ip}:{port}, db: {db_name}, collection: {collection_name}"
    )


def update_data_to_mongodb(
    data, mongo_ip="localhost", port=27017, db_name="noradb", collection_name="openalex"
):
    """
    For test case, update is made based on 'id'. Although there is a risk of works being merged,
    I'm not addressing the issue here:
    https://docs.openalex.org/how-to-use-the-api/get-single-entities#merged-entity-ids
    """
    client = MongoClient(mongo_ip, port)
    db = client[db_name]
    collection = db[collection_name]
    for work in data:
        if collection.count_documents({"id": work["id"]}, limit=1) != 0:
            logger.info(f"updating document with id: {work['id']}")
            collection.update_one({"id": work["id"]}, work)
        else:
            logger.info(f"inserting document with id: {work['id']}")
            collection.insert_one(work)
    client.close()
    logger.info(f"update to mongodb sucessful, data num: {len(data)}")
    logger.info(
        f"mongodb info. IP: {mongo_ip}:{port}, db: {db_name}, collection: {collection_name}"
    )


if __name__ == "__main__":
    fire.Fire()
