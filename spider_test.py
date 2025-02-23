import json
from pathlib import Path
import time
import requests
from utils import convert_to_seconds, get_logger, DEBUG, getRateLimit
# from MysqlEngine import DBEngine, KLine
from MongoEngine import DBEngine, KLine
from multiprocessing.pool import ThreadPool
from traceback import print_exc
# from line_profiler import LineProfiler
import argparse


API = "https://data-api.binance.vision"

"""
response = [
  [
    1499040000000,      // k线开盘时间(long)(primary-key)
    "0.01634790",       // 开盘价(decimal)
    "0.80000000",       // 最高价(decimal)
    "0.01575800",       // 最低价(decimal)
    "0.01577100",       // 收盘价(当前K线未结束的即为最新价)(decimal)
    "148976.11427815",  // 成交量(decimal)
    1499644799999,      // k线收盘时间(long)
    "2434.19055334",    // 成交额(decimal)
    308,                // 成交笔数(int)
    "1756.87402397",    // 主动买入成交量(decimal)
    "28.46694368",      // 主动买入成交额(decimal)
    "0"                 // 请忽略该参数
  ]
]

table =   (
    "id",               // 自增id(int)(primary-key)       
    "STOCK_NAME",         // 股票名称(string)(create index)
    1499040000000,      // k线开盘时间(long)(create index)
    1,                // k线开盘间隔/ms(int)(create index)
    0.01634790,       // 开盘价(decimal)
    0.80000000,       // 最高价(decimal)
    0.01575800,       // 最低价(decimal)
    0.01577100,       // 收盘价(当前K线未结束的即为最新价)(decimal)
    148976.11427815,  // 成交量(decimal)
    1499644799999,      // k线收盘时间(long)
    2434.19055334,    // 成交额(decimal)
    308,                // 成交笔数(int)
    1756.87402397,    // 主动买入成交量(decimal)
    28.46694368,      // 主动买入成交额(decimal)
  )
"""

ROUTERS = {
    "UIK": {
        "r": "/api/v3/uiKlines",
        "method": "GET",
    },
}

SYMBOL_START = {
    # "BTCUSDT": 1502942428000,
    # "ETHUSDT": 1502942429000,
    # "SOLUSDT": 1597125600000,
}


UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"


def processInterval(
    url,
    session,
    db,
    symbol,
    interval,
    interval_s,
    open_time,
    reqLimit,
    reqRateLimit,
    earlySleepInterval,
):
    try:
        resp = session.get(
            url,
            params={
                "symbol": "BTCUSDT",
                "interval": interval,
                "startTime": open_time,
                "limit": reqLimit,
            },
        )
        if resp.status_code != 200:
            if resp.status_code == 429 or "Retry-After" not in resp.headers:
                log.warning(
                    "receive too many requests from server, spider should be shutdown to avoid ban"
                )
                exit(3)  # exit with code 3 to indicate spider should be shutdown
            # get Retry-After from header
            retry_after = int(resp.headers.get("Retry-After"))
            log.info(
                "receive too many requests from server, sleep for %d seconds",
                retry_after,
            )
            time.sleep(retry_after + 5)

        data = json.loads(resp.text)
        reqRate = int(resp.headers["x-mbx-used-weight-1m"])
        log.info(
            "get [%s] from %d with interval <%s> for %d line(s) exceeded [%0.4d/%d] reqRate",
            symbol,
            open_time,
            interval,
            len(data),
            reqRate,
            reqRateLimit,
        )
        kline_list = [
            KLine(
                stock_name=symbol,
                open_time=entry[0],
                interval=interval_s,
                open_price=float(entry[1]),
                high_price=float(entry[2]),
                low_price=float(entry[3]),
                close_price=float(entry[4]),
                volume=float(entry[5]),
                close_time=entry[6],
                quote_asset_volume=float(entry[7]),
                number_of_trades=entry[8],
                taker_buy_base_asset_volume=float(entry[9]),
                taker_buy_quote_asset_volume=float(entry[10]),
            )
            for entry in data
        ]
        db.insertKLineList(kline_list)
        log.info("inserted %d line(s) into db", len(kline_list))
        # pass
    except Exception:
        print_exc()
    finally:
        if reqRate >= reqRateLimit * 0.98:
            log.info(
                "sleep for %d seconds to avoid exceeding rate limit",
                earlySleepInterval,
            )
            time.sleep(earlySleepInterval)

def get_args():
    pass


if __name__ == "__main__":
    # db = DBEngine(ip="10.68.34.200")
    db = DBEngine(ip="172.17.0.8")
    log = get_logger(title="BN-SPIDER", console_level=DEBUG)

    session = requests.Session()
    session.headers.update({"User-Agent": UA})

    reqWeight = 2
    rateLimit = getRateLimit("REQUEST_WEIGHT")[0]
    reqRateLimit = rateLimit.getLimit() // reqWeight
    sleepInterval = rateLimit.interval / rateLimit.limit
    earlySleepInterval = sleepInterval * 10

    reqLimit = 1000
    interval = "1s"
    interval_s = convert_to_seconds(interval)
    interval_ms = interval_s * 1000

    url = API + ROUTERS["UIK"]["r"]

    with ThreadPool(64) as POOL:
        for symbol, open_time in SYMBOL_START.items():
            # log.debug("check [%s] from %d with interval <%s>", symbol, open_time, interval)
            # while db.queryExistsByNameAndTime(symbol, open_time, interval_s):
            #     # log.debug(
            #     #     "skip %s from %d with interval %s",
            #     #     symbol,
            #     #     open_time,
            #     #     interval,
            #     # )
            #     open_time += interval_ms
            db_open_time = db.getMaxOpenTime(symbol, interval_s)
            if db_open_time is not None and db_open_time >= open_time:
                open_time = db_open_time + interval_ms
            log.debug(
                "start get [%s] from %d with interval <%s>", symbol, open_time, interval
            )
            while open_time <= time.time() * 1000:
                POOL.apply_async(
                    processInterval,
                    args=(
                        url,
                        session,
                        db,
                        symbol,
                        interval,
                        interval_s,
                        open_time,
                        reqLimit,
                        reqRateLimit,
                        earlySleepInterval,
                    ),
                )
                # processInterval(
                #     url,
                #     session,
                #     db,
                #     symbol,
                #     interval,
                #     interval_s,
                #     open_time,
                #     reqLimit,
                #     reqRateLimit,
                #     earlySleepInterval,
                # )
                open_time += interval_ms * reqLimit
                time.sleep(sleepInterval)
        

    
