import subprocess
from multiprocessing import Pool
import argparse
from pprint import pprint


# SYMBOL_START = {
#     "BTCUSDT": 1502942428000,
#     "ETHUSDT": 1502942429000,
#     "SOLUSDT": 1597125600000,
#     "BNBUSDT": 1509940463000,
#     "XRPUSDT": 1525421491000,
#     "WIFUSDT": 1709647200000,
#     "DOGEUSDT": 1562328001000,
#     "PEPEUSDT": 1683309600000,
#     "SPELLUSDT": 1640332800000,
#     "SUIUSDT": 1683115200000,
#     "ADAUSDT": 1523937753000,
#     "RVNUSDT": 1569412812000,
# }

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


TIME_GRID = [
    # "1s",
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
    "3d",
    "1w",
]

# check_mode = False
# # check_mode = "c"
# # check_mode = "u"
# THREADS = 16
# log_level = "DEBUG"
# num_process = 4


def run_spider(
    symbol,
    interval,
    THREADS,
    db_ip,
    db_port,
    db_type,
    db_name="binance",
    log_level="DEBUG",
    check_mode=False,
    debug=False,
):
    cmd = [
        "python",
        "dataset/bn_spider/bnspider.py",
        "--db-ip",
        db_ip,
        "--db-port",
        f"{db_port}",
        "--db-type",
        db_type,
        "--db-name",
        db_name,
        "--symbol",
        symbol,
        "--interval",
        interval,
        "--thread",
        f"{THREADS}",
        "--log-level",
        log_level,
    ]
    if check_mode == "c":
        cmd.append("--check-mode")
    elif check_mode == "u":
        cmd.append("--unique-mode")
    if debug:
        cmd.append("--debug")
    subprocess.run(cmd, check=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--check-mode",
        type=str,
        default=None,
        choices=["c", "u"],
        help="check mode or unique mode",
    )
    parser.add_argument(
        "--num-process",
        type=int,
        default=4,
        help="number of process",
    )
    parser.add_argument(
        "--thread",
        type=int,
        default=16,
        help="thread num",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="DEBUG",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="log level",
    )
    parser.add_argument(
        "--db-type",
        type=str,
        default="mongo",
        choices=["mongo", "mysql"],
        help="database type",
    )
    parser.add_argument(
        "--db-ip",
        type=str,
        default="192.168.101.14",
        help="database ip",
    )
    parser.add_argument(
        "--db-port",
        type=int,
        default=27017,
        help="database port",
    )
    parser.add_argument(
        "--db-name",
        type=str,
        default="binance",
        help="database name",
    )
    parser.add_argument(
        "--sb-a",
        type=int,
        default=0,
        help="symbol start",
    )
    parser.add_argument(
        "--sb-n",
        type=int,
        default=6,
        help="symbol end",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="debug mode",
    )
    args = parser.parse_args()
    db_ip = args.db_ip
    db_port = args.db_port
    db_type = args.db_type
    db_name = args.db_name
    check_mode = args.check_mode
    # check_mode = "c"
    # check_mode = "u"
    THREADS = args.thread
    log_level = args.log_level
    num_process = args.num_process
    a = args.sb_a
    b = args.sb_a + args.sb_n
    if b > len(SYMBOL_START):
        b = len(SYMBOL_START)
    symbolList = list(SYMBOL_START.keys())[a:b]

    # print info
    pprint(
        f"db_info: {db_type}//{db_ip}:{db_port}",
    )
    print(symbolList)
    # input("Press Enter to continue...")

    with Pool(processes=num_process) as pool:
        # pool.starmap(run_spider, [(symbol, interval)for symbol in SYMBOL_START for interval in TIME_GRID ])
        for interval in TIME_GRID:
            for symbol in symbolList:
                if args.debug:
                    run_spider(
                        symbol,
                        interval,
                        THREADS,
                        db_ip,
                        db_port,
                        db_type,
                        db_name,
                        log_level,
                        check_mode,
                        debug=True,
                    )
                # run_spider(symbol, interval, check_mode)
                else:
                    pool.apply_async(
                        run_spider,
                        (
                            symbol,
                            interval,
                            THREADS,
                            db_ip,
                            db_port,
                            db_type,
                            db_name,
                            log_level,
                            check_mode,
                        ),
                    )

        pool.close()
        pool.join()


# subprocess.run(
#     [
#         "python",
#         "dataset/bn_spider/bnspider.py",
#         "--db-type",
#         "mongo",
#         "--db-ip",
#         "192.168.101.14",
#         "--symbol",
#         "BTCUSDT",
#         "--thread",
#         "16",
#         "--interval",
#         "1d",
#     ]
# )
