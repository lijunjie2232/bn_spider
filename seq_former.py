from utils import convert_to_seconds
from MongoEngine import ValidSeq, DBEngine
from tqdm import tqdm

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
