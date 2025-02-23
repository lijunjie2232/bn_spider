# bn_spider
binance market info spider to sql

# Usage

## install

```bash
pip install -r requirements.txt
```

## show options

- bnspider.py is for single stock and single time
```bash
python bnspider.py -h
```

- bnspider_runner.py is for multiple stocks with multiple times
```bash
python bnspider_runner.py -h
```

## Configuration

- write options including sql options into cmd list in function run_spider in bnspider_runner.py ans run

## run spider

```bash
python dataset/bn_spider/bnspider_runner.py --sb-n 38 --num-process 8 --thread 16
```

## check mode

```bash
python dataset/bn_spider/bnspider_runner.py --sb-n 38 --num-process 24 --thread 2 --check-mode c
```

## calc time continuous sequence for mongodb

```bash
python MongoEngine.py
```