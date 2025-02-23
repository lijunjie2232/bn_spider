# db.klines.dropIndex("stock_name_1_open_time_1_interval_1")

mkdir -p /home/ljj/data/docker/mongo/{db,log}
docker run --restart=always --name mongo \
    -p 27017:27017 \
    -e TZ=Asia/Shanghai \
    --privileged=true \
    -e MONGO_INITDB_ROOT_USERNAME=root \
    -e MONGO_INITDB_ROOT_PASSWORD=root \
    -d mongo
docker cp mongo:/data/db /home/ljj/data/docker/mongo/
chmod -R 777 /home/ljj/data/docker/mongo/{db,log}
docker rm -f mongo
docker run --restart=always -itd --name mongo \
    -p 27017:27017 \
    -e TZ=Asia/Shanghai \
    -v /data/mongo/data:/data/db \
    -v /data/mongo/log:/data/log \
    --privileged=true \
    -e MONGO_INITDB_ROOT_USERNAME=root \
    -e MONGO_INITDB_ROOT_PASSWORD=root \
    -d mongo \
    --wiredTigerCacheSizeGB 200