import csv
from MongoEngine import DBEngine  # 导入DBEngine类


def query_and_save_to_csv(
    DB_INFO, stock_name, interval, output_file, order_by="open_time", descending=False
):
    db_engine = DBEngine(**DB_INFO)

    # 调用queryByNameAndInterval方法获取数据
    query_result = db_engine.queryByNameAndInterval(
        stock_name, interval, order_by, descending
    )

    # 将查询结果写入CSV文件
    with open(output_file, mode="w", newline="") as file:
        writer = csv.writer(file)
        # 写入表头
        writer.writerow(
            [
                "stock_name",
                "open_time",
                "interval",
                "open_price",
                "high_price",
                "low_price",
                "close_price",
                "volume",
                "close_time",
                "quote_asset_volume",
                "number_of_trades",
                "taker_buy_base_asset_volume",
                "taker_buy_quote_asset_volume",
            ]
        )
        # 写入数据行
        for kline in query_result:
            writer.writerow(
                [
                    kline.stock_name,
                    kline.open_time,
                    kline.interval,
                    kline.open_price,
                    kline.high_price,
                    kline.low_price,
                    kline.close_price,
                    kline.volume,
                    kline.close_time,
                    kline.quote_asset_volume,
                    kline.number_of_trades,
                    kline.taker_buy_base_asset_volume,
                    kline.taker_buy_quote_asset_volume,
                ]
            )


# 新增：调用distinct_query获取stock_name和interval列表
def get_distinct_stock_names_and_intervals(DB_INFO):
    db_engine = DBEngine(**DB_INFO)
    # 获取stock_name列表
    stock_names = db_engine.distinct_query("kline", {}, "stock_name", ["stock_name"])
    # stock_names = [item["_id"] for item in stock_names]

    # 获取interval列表
    intervals = db_engine.distinct_query("kline", {}, "interval", ["interval"])
    # intervals = [item["_id"] for item in intervals]

    return stock_names, intervals


# 示例调用
if __name__ == "__main__":
    from pathlib import Path
    from multiprocessing import Pool

    ROOT = Path(__file__).parent.resolve()
    CSV_DIR = ROOT / "csv"
    CSV_DIR.mkdir(exist_ok=True)
    stock_name = "BTCUSDT"
    interval = 60 * 24
    DB_INFO = {
        "ip": "192.168.101.14",
        "port": 27017,
        "user": "root",
        "password": "root",
        "db": "binance",
    }
    

    # 获取stock_name和interval列表
    stock_names, intervals = get_distinct_stock_names_and_intervals(DB_INFO)
    print("Stock Names:", stock_names)
    print("Intervals:", intervals)

    query_and_save_to_csv(
        DB_INFO,
        stock_name,
        interval,
        CSV_DIR / f"{stock_name}_{interval}.csv",  # 输出文件路径
    )
