from mongoengine import (
    Document,
    StringField,
    IntField,
    FloatField,
    DateTimeField,
    ObjectIdField,
    connect,
)

from datetime import datetime
import traceback
import sys  # 添加导入sys模块

# from MongoTables import KLine
from tqdm import tqdm
from pymongo import InsertOne, UpdateOne
from pymongo.errors import BulkWriteError
from bson import ObjectId
from pprint import pprint
from typing import Iterable


class DBEngine:
    __DBTABLES__ = {}

    def __init__(
        self,
        ip="127.0.0.1",
        port=27017,
        user="root",
        password="root",
        db="binance",
    ):
        self.db = db

        # 连接到 MongoDB 数据库
        self.connect = connect(
            host=ip,
            port=port,
            username=user,
            password=password,
            maxPoolSize=32,
            db=self.db,
        )
        # 连接已经通过 connect 函数完成

        # 创建索引
        for table in self.__DBTABLES__.values():
            table.ensure_indexes()

    def insertKLine(self, kline):
        # 检查是否存在相同的 (stock_name, open_time, interval)
        existing_kline = (
            DBEngine.__DBTABLES__["KLine"]
            .objects(
                stock_name=kline.stock_name,
                open_time=kline.open_time,
                interval=kline.interval,
            )
            .first()
        )

        if existing_kline:
            print(
                f"KLine for {kline.stock_name} at {kline.open_time} with interval {kline.interval} already exists."
            )
            return False

        # 插入新数据
        try:
            kline.save()
            print(
                f"KLine for {kline.stock_name} at {kline.open_time} with interval {kline.interval} inserted successfully."
            )
            return True
        except Exception as e:
            traceback.print_exc()
            return False

    def insertKLineList(self, kline_list: list):
        try:
            # DBEngine.__DBTABLES__["KLine"].objects.insert(kline_list)
            # 获取 pymongo 的集合对象
            if isinstance(kline_list, KLine):
                kline_list = [kline_list]
            collection = self.connect[self.db][
                self.getTable("KLine")._meta["collection"]
            ]

            operations = [InsertOne(kline.to_mongo()) for kline in kline_list]

            # 执行 bulk_write
            collection.bulk_write(operations, ordered=False)
        except BulkWriteError as bwe:
            # pprint(bwe.details)
            traceback.print_exc()

    def queryExistsByNameAndTime(self, stock_name, open_time, interval):
        existing_kline = (
            DBEngine.__DBTABLES__["KLine"]
            .objects(stock_name=stock_name, open_time=open_time, interval=interval)
            .first()
        )
        return existing_kline is not None
    
    def queryByNameAndInterval(self, stock_name, interval, order_by="open_time", descending=False, batch_size=-1):
        query = DBEngine.__DBTABLES__["KLine"].objects(stock_name=stock_name, interval=interval)
        if descending:
            order_by = f"-{order_by}"
        query = query.order_by(order_by)
        if batch_size > 1:
            query = query.batch_size(batch_size).batch_size(batch_size).as_pymongo()
        return query
    
    def VSeqExists(self, stock_name, open_time, interval):
        existing_kline = (
            DBEngine.__DBTABLES__["ValidSeq"]
            .objects(stock_name=stock_name, open_time=open_time, interval=interval)
            .first()
        )
        return existing_kline is not None

    def getMaxOpenTime(self, stock_name, interval):
        max_open_time = (
            DBEngine.__DBTABLES__["KLine"]
            .objects(stock_name=stock_name, interval=interval)
            .order_by("-open_time")  # "-" means descending
            .first()
        )

        return max_open_time.open_time if max_open_time else None

    def getMinOpenTime(self, stock_name, interval):
        min_open_time = (
            DBEngine.__DBTABLES__["KLine"]
            .objects(stock_name=stock_name, interval=interval)
            .order_by("open_time")  # "+" means ascending
            .first()
        )
        return min_open_time.open_time if min_open_time else None

    def check_kline_unique(self, stock_name=None, interval=-1):
        print(
            f"Checking [{stock_name}] for duplicate entries with interval <{interval}>..."
        )
        match = {
            "count": {
                "$gt": 1,
            }
        }
        if stock_name:
            match["stock_name"] = stock_name
        if interval > 0:
            match["interval"] = interval

        pipeline = [
            {
                "$group": {
                    "_id": {
                        "stock_name": "$stock_name",
                        "open_time": "$open_time",
                        "interval": "$interval",
                    },
                    "unique_ids": {"$addToSet": "$_id"},
                    "count": {"$sum": 1},
                }
            },
            {"$match": match},
        ]

        duplicates = DBEngine.__DBTABLES__["KLine"].objects.aggregate(pipeline)

        for duplicate in duplicates:
            # 保留第一个 _id，删除其他重复项

            unique_ids = duplicate["unique_ids"]
            unique_ids.pop(0)
            DBEngine.__DBTABLES__["KLine"].objects(id__in=unique_ids).delete()
            print(f"Duplicate entries removed for {duplicate['_id']}")

        print("Duplicate entries removed.")

    def check_continuous(self, stock_name, interval, o_time=None):
        # 查询出指定 stock_name 和 interval 的所有 open_time
        interval_ms = interval * 1000

        klines = (
            DBEngine.__DBTABLES__["KLine"]
            .objects(stock_name=stock_name, interval=interval)
            .only("open_time")
            .order_by("open_time")
        )

        expected_time = o_time

        for kline in klines:
            if expected_time is None:
                # expected_time = kline.open_time
                pass
            elif kline.open_time == expected_time:
                pass
            elif kline.open_time < expected_time:
                continue
            elif kline.open_time > expected_time:
                print(
                    f"find missing interval: [{stock_name}] from {expected_time} to {kline.open_time} with interval {interval}"
                )
                yield (
                    expected_time,
                    int((kline.open_time - expected_time) // interval_ms),
                )
            expected_time = kline.open_time + interval_ms

    def getContinuousSeq(self, stock_name, interval, o_time=None):
        # 查询出指定 stock_name 和 interval 的所有 open_time
        interval_ms = interval * 1000

        klines = (
            DBEngine.__DBTABLES__["KLine"]
            .objects(stock_name=stock_name, interval=interval)
            .only("open_time")
            .order_by("open_time")
        )

        start = o_time
        expected_time = o_time

        for kline in klines:
            if start is None:
                start = kline.open_time
            if expected_time is None:
                pass
            elif kline.open_time == expected_time:
                pass
            elif kline.open_time < expected_time:
                continue
            elif kline.open_time > expected_time:
                print(
                    f"find missing interval: [{stock_name}] from {expected_time} to {kline.open_time} with interval {interval}"
                )
                yield (
                    start,
                    # int((expected_time - start) // interval_ms),
                    expected_time,
                )
                start = kline.open_time
            expected_time = kline.open_time + interval_ms
        if expected_time is not None:
            yield (
                start,
                # int((expected_time - start) // interval_ms),
                expected_time,
            )

    def getLength(self, stock_name, interval, batch_size=1024 * 16):
        # 使用聚合框架优化 count 操作
        pipeline = [
            {"$match": {"stock_name": stock_name, "interval": interval}},
            {"$count": "length"},
        ]
        result = DBEngine.__DBTABLES__["KLine"].objects.aggregate(pipeline)
        result = list(result)
        return result[0]["length"] if result else 0

    @classmethod
    def register_table(cls, table_class):
        if table_class.__name__ not in cls.__DBTABLES__:
            # cls.__DBTABLES__.append(table_class)
            cls.__DBTABLES__[table_class.__name__] = table_class
        else:
            raise Exception(
                f"Table {table_class.__name__} has already been registered as: "
                + cls.__DBTABLES__[table_class.__name__]
            )
        return table_class

    def getTable(self, table_name):
        return DBEngine.__DBTABLES__[table_name]

    def insertPeople(self, people):
        try:
            # DBEngine.__DBTABLES__["Person"].objects.insert(people)
            # 获取 pymongo 的集合对象
            if isinstance(people, Person):
                people = [people]
            collection = self.connect[self.db][
                self.getTable("Person")._meta["collection"]
            ]

            operations = [InsertOne(person.to_mongo()) for person in people]

            # 执行 bulk_write
            collection.bulk_write(operations, ordered=False)
        except BulkWriteError as bwe:
            # pprint(bwe.details)
            traceback.print_exc()

    def getAllPerson(self):
        return DBEngine.__DBTABLES__["Person"].objects.all()

    def update(self, objects):
        try:
            if not isinstance(objects, Iterable):
                objects = [objects]
            operations = [
                UpdateOne(
                    {
                        "stock_name": object.stock_name,
                        "open_time": object.open_time,
                        "interval": object.interval,
                    },
                    {"$set": object.to_mongo()},
                    upsert=False,
                )
                for object in objects
            ]

            collection = self.connect[self.db][
                self.getTable(objects[0].__class__.__name__)._meta["collection"]
            ]

            # 执行 bulk_write
            collection.bulk_write(operations, ordered=False)
        except Exception as e:
            traceback.print_exc()

    def insert(self, objects):
        try:
            if not isinstance(objects, Iterable):
                objects = [objects]
            operations = [InsertOne(object.to_mongo()) for object in objects]

            collection = self.connect[self.db][
                self.getTable(objects[0].__class__.__name__)._meta["collection"]
            ]

            # 执行 bulk_write
            collection.bulk_write(operations, ordered=False)
        except Exception as e:
            traceback.print_exc()

    def distinct_query(self, collection_name, query, distinct_field, fields):
        # 获取 pymongo 的集合对象
        collection = self.connect[self.db][collection_name]

        # 构建聚合管道
        pipeline = [
            {"$match": query},
            {"$group": {"_id": f"${distinct_field}", "data": {"$first": "$$ROOT"}}},
            {"$project": {field: 1 for field in fields}}
        ]

        # 执行聚合查询
        result = collection.aggregate(pipeline)

        # 返回结果
        return  [item["_id"] for item in result]


# 定义文档类
@DBEngine.register_table
class KLine(Document):
    # _id = ObjectIdField(required=False)
    stock_name = StringField(required=True)
    open_time = IntField(required=True)
    interval = IntField(required=True)
    open_price = FloatField(required=True)
    high_price = FloatField(required=True)
    low_price = FloatField(required=True)
    close_price = FloatField(required=True)
    volume = FloatField(required=True)
    close_time = IntField(required=True)
    quote_asset_volume = FloatField(required=True)
    number_of_trades = IntField(required=True)
    taker_buy_base_asset_volume = FloatField(required=True)
    taker_buy_quote_asset_volume = FloatField(required=True)

    meta = {
        "indexes": [
            {
                "fields": ["stock_name", "open_time", "interval"],
                "unique": True,
            },
            {
                "fields": ["interval", "stock_name", "open_time"],
                "unique": True,
            },
        ],
        "collection": "kline",
        "index_background": True,
    }

    def __str__(self):
        return (
            f"KLine["
            + f"stock_name={self.stock_name},"
            + f"open_time={self.open_time},"
            + f"interval={self.interval},"
            + f"close_time={self.close_time},"
            + f"close_price={self.close_price},"
            + f"volume={self.volume},"
            + f"number_of_trades={self.number_of_trades},"
            + f"taker_buy_base_asset_volume={self.taker_buy_base_asset_volume},"
            + f"taker_buy_quote_asset_volume={self.taker_buy_quote_asset_volume}"
            + "]"
        )

    def __repr__(self):
        return super().__str__()


@DBEngine.register_table
class ValidSeq(Document):
    stock_name = StringField(required=True)
    interval = IntField(required=True)
    open_time = IntField(required=True)
    length = IntField(required=True)

    meta = {
        "indexes": [
            {
                "fields": ["stock_name", "interval", "open_time"],
                "unique": True,
            },
        ],
        "collection": "vseq",
        "index_background": True,
    }

    def __str__(self):
        return (
            "ValidSeq["
            + f"stock_name={self.stock_name},"
            + f"interval={self.interval},"
            + f"open_time={self.open_time},"
            + f"length={self.length}"
        )

    def __repr__(self):
        return super().__str__()


# 定义文档类
# this class is for mongodb test
@DBEngine.register_table
class Person(Document):
    pid = IntField(required=True)
    name = StringField(required=True)

    meta = {
        "indexes": [
            {
                "fields": ("pid", "name"),
                "unique": True,
            },
        ],
        "collection": "people",
        "index_background": True,
    }

    def __str__(self):
        return f"Person[pid={self.pid}, name={self.name}]"

    def __repr__(self):
        return super().__str__()


if __name__ == "__main__":
    from utils import convert_to_seconds

    # db = DBEngine(ip="10.68.34.200")
    # interval = convert_to_seconds("1s")
    # db = DBEngine(ip="192.168.101.14")
    # print(db.getMaxOpenTime("BTCUSDT", 1))
    # print(db.getLength("BTCUSDT", interval))
    # missing_intervals = db.check_continuous("BTCUSDT", 1)
    # for i in missing_intervals:
    #     continue
    # pass

    # # drop Person
    # personList = [
    #     Person(pid=1, name="zhangsan"),
    #     Person(pid=2, name="lisi"),
    #     Person(pid=3, name="wangwu"),
    #     Person(pid=4, name="zhaoliu"),
    # ]

    # personList2 = [Person(pid=2, name="lisi"), Person(pid=6, name="zhaoliu")]
    # db.insertPeople(personList)
    # db.insertPeople(personList2)
    # for person in db.getTable("Person").objects:
    #     print(person)

    def seqMaker(symbol, interval):
        db = DBEngine(ip="192.168.101.14")
        validList = []
        # start = db.getMinOpenTime(symbol, interval)
        # for open_time, interval_counts in db.check_continuous(symbol, interval):
        #     # print(f"Missing {interval_counts} intervals at {open_time}")
        #     validList.append((start, (open_time - start) // interval // 1000))
        #     start = open_time + interval * interval_counts * 1000
        # validList.append(
        #     (
        #         start,
        #         (db.getMaxOpenTime(symbol, interval) - start) // interval // 1000,
        #     )
        # )

        # pprint(validList)
        # return validList  # , symbol, interval
        valid_sequences = []
        seq = db.getContinuousSeq(symbol, interval)
        for time_a, time_b in seq:
            valid_sequences.append((time_a, time_b))
            # only update if exists (symbol, interval, time_a)
            vseq = ValidSeq(
                stock_name=symbol,
                interval=interval,
                open_time=time_a,
                length=(time_b - time_a) // interval // 1000,
            )
            if db.VSeqExists(symbol, time_a, interval):
                db.update([vseq])
                print("update", vseq)
            else:
                db.insert([vseq])
                print("insert", vseq)
        return valid_sequences

    from bnspider_runner import run_spider, SYMBOL_START, TIME_GRID

    # from multiprocessing.pool import ThreadPool
    from multiprocessing import Pool

    debug = False
    missing_data = {}
    symbolList = list(SYMBOL_START.keys())#[15:18]
    timeList = TIME_GRID#[4:7]
    num_process = len(symbolList) * len(timeList)
    loop = tqdm(desc="find missing data", total=num_process)

    def collect_missing_data(result, sym, inter):
        if inter not in missing_data:
            missing_data[inter] = {}
        if sym not in missing_data[inter]:
            missing_data[inter][sym] = []
        missing_data[inter][sym].extend(result)
        loop.update()

    with Pool(processes=num_process) as pool:
        for interval in timeList:
            interval_s = convert_to_seconds(interval)
            for symbol in symbolList:
                if debug:
                    missing_sequences = seqMaker(
                        # db,
                        symbol,
                        interval_s,
                    )
                    collect_missing_data(missing_sequences, symbol, interval_s)
                else:
                    pool.apply_async(
                        seqMaker,
                        (
                            # db,
                            symbol,
                            interval_s,
                        ),
                        callback=lambda result, sym=symbol, inter=interval_s: collect_missing_data(
                            result, sym, inter
                        ),
                    )

        pool.close()
        pool.join()
    loop.close()

    db = DBEngine(ip="192.168.101.14")
    # 绘制时间线
    import matplotlib.pyplot as plt

    for interval, symbols in missing_data.items():
        plt.figure(figsize=(15, 30))
        for symbol, valid_sequences in symbols.items():
            # valid_sequences = []
            start_time = db.getMinOpenTime(symbol, interval)
            end_time = db.getMaxOpenTime(symbol, interval)
            # current_time = start_time

            # if start_time is None or end_time is None:
            #     continue

            # for missing_start, missing_count in missing_sequences:
            #     if current_time < missing_start:
            #         valid_sequences.append((current_time, missing_start))
            #     current_time = missing_start + missing_count * interval * 1000

            # if current_time < end_time:
            #     valid_sequences.append((current_time, end_time))

            """
                stock_name = StringField(required=True)
                interval = IntField(required=True)
                open_time = IntField(required=True)
                length = IntField(required=True)
            """
            # for time_a, time_b in valid_sequences:
            #     # only update if exists (symbol, interval, time_a)
            #     vseq = ValidSeq(
            #         stock_name=symbol,
            #         interval=interval,
            #         open_time=time_a,
            #         length=(time_b - time_a) // interval // 1000,
            #     )
            #     if db.VSeqExists(symbol, time_a, interval):
            #         db.update([vseq])
            #         print("update", vseq)
            #     else:
            #         db.insert([vseq])
            #         print("insert", vseq)

            # for start, end in valid_sequences:
            #     plt.plot([start, end], [symbolList.index(symbol), symbolList.index(symbol)], label=f'{symbol} valid sequence')
            # 提取时间点
            times = [[start, end] for start, end in valid_sequences]
            # times.append(end_time)  # 添加最后一个有效时间点

            # 提取对应的股票索引
            symbol_index = symbolList.index(symbol)
            # indices = [symbol_index] * len(times)

            # label = f"{symbol} valid sequence"
            for i in times:
                plt.plot(i, [symbol_index] * 2, marker="o", linestyle="-")
            # # 绘制连点成线
            # plt.plot(
            #     times,
            #     indices,
            #     marker="o",
            #     linestyle="-",
            #     label=f"{symbol} valid sequence",
            # )

        plt.title(f"Valid Data Timeline for Interval {interval}")
        plt.xlabel("Time")
        plt.ylabel("Symbol Index")
        plt.yticks(range(len(symbolList)), symbolList)
        # plt.legend()
        plt.savefig(f"valid_timeline_{interval}.png")
