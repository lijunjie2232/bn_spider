import json
from pathlib import Path
import time
import requests
from utils import convert_to_seconds, get_logger, getRateLimit, get_log_level

# from MysqlEngine import DBEngine, KLine
# from MongoTables import KLine
from multiprocessing.pool import ThreadPool
from traceback import print_exc

# from line_profiler import LineProfiler
import argparse
import gc


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
    "BTCUSDT": 1502942428000,
    "ETHUSDT": 1502942429000,
    "SOLUSDT": 1597125600000,
    "BNBUSDT": 1509940463000,
    "XRPUSDT": 1525421491000,
    "WIFUSDT": 1709647200000,
    "DOGEUSDT": 1562328001000,
    "PEPEUSDT": 1683309600000,
    "SPELLUSDT": 1640332800000,
    "SUIUSDT": 1683115200000,
    "ADAUSDT": 1523937753000,
    "RVNUSDT": 1569412812000,
    "JUVUSDT": 1608530400000,
    "OMUSDT": 1615194000000,
    "LTCUSDT": 1513135954000,
    "CREAMUSDT": 1695369600000,
    "ACMUSDT": 1614164400000,
    "CITYUSDT": 1636531200000,
    "TSTUSDT": 1739098800000,
    "ATMUSDT": 1609308000000,
    "USUALUSDT": 1732010400000,
    "DATAUSDT": 1586264400000,
    "PORTOUSDT": 1637064000000,
    "BARUSDT": 1619002800000,
    "TRXUSDT": 1528716605000,
    "XLMUSDT": 1527759034000,
    "LINKUSDT": 1547632852000,
    "JUPUSDT": 1706716800000,
    "BNXUSDT": 1677052800000,
    "PNUTUSDT": 1731319200000,
    "CAKEUSDT": 1613714400000,
    "SHIBUSDT": 1620644400000,
    "WBTCUSDT": 1682676000000,
    "AVAXUSDT": 1600756200000,
    "HBARUSDT": 1569729600000,
    "TONUSDT": 1723111200000,
    "DOTUSDT": 1597791600000,
}


UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"


def processInterval(
    url,
    session,
    db,
    log,
    symbol,
    interval,
    interval_s,
    open_time,
    reqLimit,
    reqRateLimit,
    earlySleepInterval,
):
    try:
        close_time = open_time + interval_s * (reqLimit) * 1000
        resp = session.get(
            url,
            params={
                "symbol": symbol,
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
        data_target = filter(lambda x: x[0] < close_time, data)
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
            for entry in data_target
        ]
        if len(kline_list) == 0:
            log.info(
                "no data found for [%s] from %d with interval <%s>",
                symbol,
                open_time,
                interval,
            )
            return
        db.insertKLineList(kline_list)
        # dbResult = db.insertKLineList(kline_list)
        # assert isinstance(dbResult, list) and len(dbResult) == len(
        #     kline_list
        # ), Exception(
        #     f"[{symbol}] insert {len(kline_list)} line(s) failed, "
        #     + f"start at <{open_time}> with interval <{interval}>, "
        #     + f"with db returns: {dbResult}"
        # )
        log.info(
            f"[{symbol}] inserted {len(kline_list)} line(s) into db "
            + f"start at <{open_time}> with interval <{interval}>"
        )
        # pass
    except Exception as e:
        print(e)
        print_exc()
    finally:
        if reqRate >= reqRateLimit * 0.95:
            log.info(
                "sleep for %d seconds to avoid exceeding rate limit",
                earlySleepInterval,
            )
            time.sleep(earlySleepInterval)
        # if len(kline_list) > 0:
            # del dbResult
        del data
        del reqRate
        del kline_list
        del resp
        del data_target
        gc.collect()


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


if __name__ == "__main__":
    args = get_args()
    # db = DBEngine(ip="10.68.34.200")
    # db = DBEngine(ip="172.17.0.8")
    if args.db_type == "mongo":
        from MongoEngine import DBEngine, KLine
    elif args.db_type == "mysql":
        from MysqlEngine import DBEngine, KLine
    else:
        raise ValueError("db_type not valid")

    db = DBEngine(
        ip=args.db_ip,
        port=args.db_port,
        user=args.db_user,
        password=args.db_password,
        db=args.db_name,
    )
    log = get_logger(
        title="BN-SPIDER",
        console_level=get_log_level(args.log_level),
    )

    session = requests.Session()
    session.headers.update({"User-Agent": UA})

    symbol = args.symbol
    assert symbol in SYMBOL_START, f"{symbol} is not a valid symbol"
    reqWeight = 2
    # rateLimit = getRateLimit("REQUEST_WEIGHT")[0]
    # reqRateLimit = rateLimit.getLimit() // reqWeight
    # sleepInterval = rateLimit.interval / rateLimit.limit
    reqRateLimit = 3000
    sleepInterval = 0.1
    earlySleepInterval = sleepInterval * 10
    log.info("get api request rate limit: %d/min", reqRateLimit)
    log.info("get api request sleep interval: %0.4f seconds", sleepInterval)

    reqLimit = 1000
    interval = args.interval
    interval_s = convert_to_seconds(interval)
    interval_ms = interval_s * 1000

    url = API + ROUTERS["UIK"]["r"]

    if args.check_mode:
        with ThreadPool(args.thread) as POOL:
            for open_time, interval_counts in db.check_continuous(symbol, interval_s):
                if interval_counts > 1000:
                    open_times = [
                        (open_time + i * interval_ms * 1000, 1000)
                        for i in range(0, interval_counts // 1000)
                    ] + [
                        (
                            open_time + interval_counts // 1000 * 1000 * interval_ms,
                            interval_counts % 1000,
                        )
                    ]
                else:
                    open_times = [(open_time, interval_counts)]
                for open_time, interval_count in open_times:
                    if args.debug:
                        processInterval(
                            url,
                            session,
                            db,
                            log,
                            symbol,
                            interval,
                            interval_s,
                            open_time - 1,
                            interval_count,
                            reqRateLimit,
                            earlySleepInterval,
                        )
                    else:
                        POOL.apply_async(
                            processInterval,
                            args=(
                                url,
                                session,
                                db,
                                log,
                                symbol,
                                interval,
                                interval_s,
                                open_time - 1,
                                interval_count,
                                reqRateLimit,
                                earlySleepInterval,
                            ),
                        )
                    time.sleep(sleepInterval)
            POOL.close()
            POOL.join()
    elif args.unique_mode:
        db.check_kline_unique(symbol, interval_s)
    else:
        with ThreadPool(args.thread) as POOL:
            open_time = SYMBOL_START[symbol]
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
                if args.debug:
                    processInterval(
                        url,
                        session,
                        db,
                        log,
                        symbol,
                        interval,
                        interval_s,
                        open_time,
                        reqLimit,
                        reqRateLimit,
                        earlySleepInterval,
                    )
                else:
                    POOL.apply_async(
                        processInterval,
                        args=(
                            url,
                            session,
                            db,
                            log,
                            symbol,
                            interval,
                            interval_s,
                            open_time,
                            reqLimit,
                            reqRateLimit,
                            earlySleepInterval,
                        ),
                    )

                open_time += interval_ms * reqLimit
                time.sleep(sleepInterval)
            POOL.close()
            POOL.join()
