import json
from pathlib import Path
import requests
from sqlalchemy import (
    create_engine,
    Column,
    String,
    BigInteger,
    Integer,
    Numeric,
    Index,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.exc import IntegrityError

from traceback import print_exc

# 创建基类
Base = declarative_base()


# 定义模型类
class KLine(Base):
    __tablename__ = "kline"

    # id = Column(Integer, primary_key=True, autoincrement=True)
    stock_name = Column(String(20), index=True)  # 修改: 指定 String 类型的长度
    open_time = Column(BigInteger, index=True)
    interval = Column(Integer, index=True)
    open_price = Column(Numeric(18, 8))
    high_price = Column(Numeric(18, 8))
    low_price = Column(Numeric(18, 8))
    close_price = Column(Numeric(18, 8))
    volume = Column(Numeric(18, 8))
    close_time = Column(BigInteger)
    quote_asset_volume = Column(Numeric(18, 8))
    number_of_trades = Column(Integer)
    taker_buy_base_asset_volume = Column(Numeric(18, 8))
    taker_buy_quote_asset_volume = Column(Numeric(18, 8))


class DBEngine:
    Base = Base

    def __init__(
        self,
        ip="127.0.0.1",
        port=3306,
        user="root",
        password="root",
        db="binance",
    ):
        # 创建数据库引擎
        self.engine = create_engine(
            f"mysql+mysqlconnector://{user}:{password}@{ip}:{port}/{db}"
        )

        self.Base.metadata.create_all(self.engine)

        # 创建会话
        Session = sessionmaker(bind=self.engine)
        self.session = scoped_session(Session)

    def insertKLine(self, kline: KLine):
        # 检查是否存在相同的 (stock_name, open_time, interval)
        existing_kline = (
            self.session.query(KLine)
            .filter_by(
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
            self.session.add(kline)
            self.session.commit()
            print(
                f"KLine for {kline.stock_name} at {kline.open_time} with interval {kline.interval} inserted successfully."
            )
            return True
        except IntegrityError as e:
            self.session.rollback()
            print_exc()
            return False
        # except Exception as e:
        #     if transaction:
        #         self.session.rollback()
        #     print(f"Error: {e}")
        #     return False

    def insertKLineList(self, kline_list: list):
        try:
            self.session.add_all(kline_list)
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            print_exc()

    def queryExistsByNameAndTime(self, stock_name, open_time, interval):
        existing_kline = (
            self.session.query(KLine)
            .filter_by(stock_name=stock_name, open_time=open_time, interval=interval)
            .first()
        )
        return existing_kline is not None

    def getMaxOpenTime(self, stock_name, interval):
        max_open_time = (
            self.session.query(KLine.open_time)
            .filter_by(stock_name=stock_name, interval=interval)
            .order_by(KLine.open_time.desc())
            .first()
        )
        return max_open_time[0] if max_open_time else None


if __name__ == "__main__":
    db = DBEngine()
    print(db.getMaxOpenTime("BTCUSDT", 1))
