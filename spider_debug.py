import subprocess
from multiprocessing import Pool

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
}

TIME_GRID = [
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
    "3d",
    "1w",
]


def run_spider(symbol, interval):
    subprocess.run(
        [
            "python",
            "dataset/bn_spider/bnspider.py",
            "--db-type",
            "mongo",
            "--db-ip",
            "192.168.101.14",
            "--symbol",
            symbol,
            "--thread",
            "16",
            "--interval",
            interval,
        ]
    )


with Pool(processes=12) as pool:
    pool.starmap(run_spider, [(symbol, interval)for symbol in SYMBOL_START for interval in TIME_GRID ])
    # for interval in TIME_GRID:
    #     for symbol in SYMBOL_START:
    #         pool.apply_async(run_spider, (symbol, interval))
    
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
