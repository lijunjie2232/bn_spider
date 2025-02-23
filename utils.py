import logging
from logging import NOTSET, DEBUG, INFO, WARNING, ERROR, CRITICAL
import requests
import json
from tqdm import tqdm
import argparse



def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", type=str, default="BTCUSDT", help="symbol name")
    parser.add_argument("--interval", type=str, default="1s", help="interval")
    parser.add_argument("--db-type", type=str, default="mongo", help="db type")
    parser.add_argument("--db-ip", type=str, default="127.0.0.1", help="db ip")
    parser.add_argument("--db-port", type=int, default=27017, help="db port")
    parser.add_argument("--db-user", type=str, default="root", help="db user")
    parser.add_argument("--db-password", type=str, default="root", help="db password")
    parser.add_argument("--db-name", type=str, default="binance", help="db name")
    parser.add_argument("--thread", type=int, default=64, help="thread num")
    parser.add_argument("--log-level", type=str, default="DEBUG", help="log level")
    parser.add_argument(
        "--check-mode", action="store_true", help="check mode to find missing data"
    )
    parser.add_argument(
        "--unique-mode", action="store_true", help="unique mode to delete duplicate data"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Activate debug mode and run training only with a subset of data.",
    )

    return parser.parse_args()

def convert_to_seconds(time_str):
    """
    Convert a time string like '1h', '1w' into seconds.
    Supported units: s (seconds), m (minutes), h (hours), d (days), w (weeks).
    """
    unit = time_str[-1]
    value = int(time_str[:-1])

    if unit == "s":
        return value
    elif unit == "m":
        return value * 60
    elif unit == "h":
        return value * 3600
    elif unit == "d":
        return value * 86400
    elif unit == "w":
        return value * 604800
    elif unit == "M":
        return value * 2592000
    else:
        raise ValueError(f"Unsupported time unit: {unit}")


def get_logger(
    title="LOG",
    level=10,
    console_format: str = "%(asctime)s - %(filename)s:%(lineno)d:%(funcName)s [%(levelname)s]: %(message)s",
    console_level: int = logging.INFO,
    file_format: str = "%(asctime)s - %(filename)s:%(lineno)d:%(funcName)s [%(levelname)s]: %(message)s",
    file_level: int = logging.NOTSET,
    file_path=None,
):
    logger = logging.getLogger(title)
    logger.setLevel(level)
    assert console_level > 0 or file_level > 0, "At least one logging level > 0"

    if console_level > 0:
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(console_level)
        console_formatter = logging.Formatter(console_format)
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

    # File handler
    if file_level > 0:
        assert file_path, "file_path must be specified"
        file_handler = logging.FileHandler(file_path)
        file_handler.setLevel(file_level)
        file_formatter = logging.Formatter(file_format)
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

    return logger


def parseTimeString(time_str):
    time_str = time_str.lower()
    if time_str == "minute":
        return 60
    if time_str == "second":
        return 1
    if time_str == "hour":
        return 3600
    if time_str == "day":
        return 86400
    if time_str == "week":
        return 604800
    if time_str == "month":
        return 2592000
    raise ValueError(f"Unsupported time unit: {time_str}")


class RateLimit:
    # {'rateLimitType': 'REQUEST_WEIGHT', 'interval': 'MINUTE', 'intervalNum': 1, 'limit': 6000}
    def __init__(self, rateLimit: dict):
        self.rateLimitType = rateLimit["rateLimitType"]
        self.interval = rateLimit["intervalNum"] * parseTimeString(
            rateLimit["interval"]
        )
        self.limit = int(rateLimit["limit"])

    def getLimit(self):
        return self.limit

    def __eq__(self, value):
        if isinstance(value, RateLimit):
            return (
                self.rateLimitType == value.rateLimitType
                and self.interval == value.interval
                and self.limit == value.limit
            )
        else:
            return False

    def __str__(self):
        return f"RateLimit(rateLimitType={self.rateLimitType}, interval={self.interval}, limit={self.limit})"

    def __repr__(self):
        return self.__str__()


def getRateLimit(limitType=None):
    """_summary_

    Args:
        limitType (_type_, optional): _description_. Defaults to None. type in ["REQUEST_WEIGHT", "ORDERS", "RAW_REQUESTS"]

    Returns:
        _type_: _description_
    """
    url = "https://data-api.binance.vision/api/v3/exchangeInfo"
    resp = requests.get(url)
    result = json.loads(resp.text)
    rateLimit = [
        RateLimit(rl)
        for rl in result["rateLimits"]
        if rl["rateLimitType"] == limitType or not limitType
    ]
    return rateLimit


def get_log_level(level: str):
    if level == "DEBUG":
        return DEBUG
    elif level == "INFO":
        return INFO
    elif level == "WARNING":
        return WARNING
    elif level == "ERROR":
        return ERROR
    elif level == "CRITICAL":
        return CRITICAL
    else:
        raise ValueError("log level not valid")


def get_open_time(url, symbolList, interval):
    timeList = []
    for symbol in tqdm(symbolList):
        try:
            resp = requests.get(url % (symbol, interval))
            result = json.loads(resp.text)
            time = result[0][0]
            timeList.append(time)
        except Exception as e:
            print(e)
            timeList.append(None)
    return timeList


if __name__ == "__main__":
    # rateLimit = getRateLimit("REQUEST_WEIGHT")
    # print(rateLimit)

    intervalList = [
        "1s",
        "1m",
        "3m",
        "5m",
        "15m",
        "30m",
        "1h",
        "2h",
        "4h",
        "6h",
        "8h",
        "12h",
        "1d",
    ]
    for interval in intervalList:
        print(convert_to_seconds(interval))

    symbolList = [
        "BTCUSDT",
        "ETHUSDT",
        "SOLUSDT",
        "BNBUSDT",
        "XRPUSDT",
        "WIFUSDT",
        "DOGEUSDT",
        "PEPEUSDT",
        "SPELLUSDT",
        "SUIUSDT",
        "ADAUSDT",
        "RVNUSDT",
        "JUVUSDT",
        "OMUSDT",
        "LTCUSDT",
        "CREAMUSDT",
        "ACMUSDT",
        "CITYUSDT",
        "TSTUSDT",
        "ATMUSDT",
        "USUALUSDT",
        "DATAUSDT",
        "PORTOUSDT",
        "BARUSDT",
        "WIFUSDT",
        "TRXUSDT",
        "XLMUSDT",
        "LINKUSDT",
        "JUPUSDT",
        "BNXUSDT",
        "PNUTUSDT",
        "CAKEUSDT",
        "SHIBUSDT",
        "WBTCUSDT",
        "AVAXUSDT",
        "HBARUSDT",
        "TONUSDT",
        "DOTUSDT",
    ]
    symbolList = list(dict.fromkeys(symbolList))
    for i in zip(
        symbolList,
        get_open_time(
            "https://data-api.binance.vision/api/v3/uiKlines?symbol=%s&startTime=737965206000&limit=10&interval=%s",
            symbolList,
            "1s",
        ),
    ):
        print(f"'{i[0]}': {i[1]}")