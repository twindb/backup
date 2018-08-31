import pymysql
from pymysql import OperationalError


def test_pymysql_connect_returns_error():
    try:
        connection = pymysql.connect()
    except OperationalError as err:
        pass
    except BaseException as err:
        pass
