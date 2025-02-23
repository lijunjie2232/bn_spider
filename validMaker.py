from MongoEngine import DBEngine
from utils import get_args, get_logger, get_log_level
from bnspider_runner import SYMBOL_START, TIME_GRID

if __name__ == "__main__":
    args = get_args()

    if args.db_type == "mongo":
        from MongoEngine import DBEngine, ValidSeq
    elif args.db_type == "mysql":
        raise Exception("not implemented")
        # from MysqlEngine import DBEngine, KLine
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

    db = DBEngine(ip="192.168.101.14")
    validList = []
    symbol = "BTCUSDT"
    interval = 60 * 60  # 1 hour
    start = db.getMinOpenTime(symbol, interval)
    for open_time, interval_counts in db.check_continuous(symbol, interval):
        print(f"Missing {interval_counts} intervals at {open_time}")
        try:
            ValidSeq(
                stock_name=symbol,
                interval=interval,
                open_time=start,
                length=(open_time - start) // interval // 1000,
            ).save()
        except Exception as e:
            print(e)
        start = open_time + interval * interval_counts * 1000
    try:
        ValidSeq(
            stock_name=symbol,
            interval=interval,
            open_time=start,
            length=(db.getMaxOpenTime(symbol, interval) - start) // interval // 1000,
        ).save()
    except Exception as e:
        print(e)
