import csv
from MongoEngine import DBEngine  # 导入DBEngine类
from utils import get_logger


def export_validseq_to_csv(
    DB_INFO,
    interval,
    output_file,
    order_by=["stock_name", "interval", "open_time"],
    descending=[False, False, False],
    batch_size=1000,
):
    """
    将 ValidSeq 表的数据导出到 CSV 文件，并按 stock_name、interval、open_time 排序。

    :param db: DBEngine 实例
    :param output_file: 输出的 CSV 文件路径
    """

    def yield_vseq(cursor, chunk_size=batch_size):
        """
        Generator to yield chunks from cursor
        :param cursor:
        :param chunk_size:
        :return:
        """
        chunk = []
        for i, vseq in enumerate(cursor):
            if i % chunk_size == 0:
                if chunk:
                    yield chunk
                    del chunk[:]
            chunk.append(
                [
                    vseq.stock_name,
                    vseq.interval,
                    vseq.open_time,
                    vseq.length,
                ]
            )
        yield chunk

    # 定义 CSV 文件的表头
    headers = ["stock_name", "interval", "open_time", "length"]

    logger = get_logger()
    db_engine = DBEngine(**DB_INFO)

    # 调用queryByNameAndInterval方法获取数据
    query_result = db_engine.queryByNameAndInterval(
        # stock_name,
        {},
        interval,
        order_by,
        descending,
        collection="ValidSeq",
        # batch_size=batch_size,
    )
    # 将查询结果写入CSV文件
    with open(output_file, mode="w", newline="") as file:
        writer = csv.writer(file)
        # 写入表头
        writer.writerow(headers)
        # 写入数据行
        for batch in yield_vseq(query_result):
            writer.writerows(batch)
            logger.info(f"Written {len(batch)} line(s) to CSV {output_file.name}")

    print(f"ValidSeq 数据已成功导出到 {output_file}")


# 新增：调用distinct_query获取stock_name和interval列表
def get_distinct_stock_names_and_intervals(DB_INFO):
    db_engine = DBEngine(**DB_INFO)

    # 获取stock_name列表
    stock_names = db_engine.distinct_query("vseq", {}, "stock_name", ["stock_name"])
    # stock_names = [item["_id"] for item in stock_names]

    # 获取interval列表
    intervals = db_engine.distinct_query("vseq", {}, "interval", ["interval"])
    # intervals = [item["_id"] for item in intervals]

    return stock_names, intervals


# 示例调用
if __name__ == "__main__":
    from pathlib import Path
    from multiprocessing import Pool
    import os

    ROOT = Path(__file__).parent.resolve()
    CSV_DIR = ROOT / "csv"
    CSV_DIR.mkdir(exist_ok=True)
    stock_name = "BTCUSDT"
    interval = 60 * 60
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

    # compare with system core num
    NUM_PROCESSES = (
        os.cpu_count() if len(intervals) > os.cpu_count() else len(intervals) + 1
    )
    with Pool(NUM_PROCESSES) as pool:
        for interval in intervals:
            pool.apply_async(
                export_validseq_to_csv,
                args=(
                    DB_INFO,
                    interval,
                    CSV_DIR / f"seq_{interval}.csv",
                ),
            )
        pool.apply_async(
            export_validseq_to_csv,
            args=(
                DB_INFO,
                {},
                CSV_DIR / f"seq_all.csv",
            ),
        )
        pool.close()
        pool.join()

    # query_and_save_to_csv(
    #     DB_INFO,
    #     stock_name,
    #     interval,
    #     CSV_DIR / f"{stock_name}_{interval}.csv",  # 输出文件路径
    # )
