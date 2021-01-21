import pytest

from loguru import logger

from workflow.scripts.utils.general import neo4j_connect


def test_connect():
    query = """
        MATCH (n)
        RETURN n LIMIT 2;
    """
    driver = neo4j_connect()
    session = driver.session()
    data = session.run(query).data()
    logger.info(data)
    assert len(data) == 2
