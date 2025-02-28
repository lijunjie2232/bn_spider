import csv
from MongoEngine import DBEngine  # 导入DBEngine类

def query_and_save_to_csv(stock_name, interval, output_file, order_by="open_time", descending=False):
    # 创建DBEngine实例
    db_engine = DBEngine(ip="127.0.0.1", port=27017, user="root", password="root", db="binance")

    # 调用queryByNameAndInterval方法获取数据
    query_result = db_engine.queryByNameAndInterval(stock_name, interval, order_by, descending)

    # 将查询结果写入CSV文件
    with open(output_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        # 写入表头
        writer.writerow(["stock_name", "open_time", "interval", "open_price", "high_price", "low_price", "close_price", "volume", "close_time", "quote_asset_volume", "number_of_trades", "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume"])
        # 写入数据行
        for kline in query_result:
            writer.writerow([
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
                kline.taker_buy_quote_asset_volume
            ])

# 示例调用
if __name__ == "__main__":
    query_and_save_to_csv("BTCUSDT", 1, "BTCUSDT_1.csv")