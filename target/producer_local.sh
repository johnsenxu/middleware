export NAMESRV_ADDR=localhost:9876

java -Drocketmq.namesrv.addr=localhost:9876 -cp preliminary.demo-1.0-SNAPSHOT.jar com.alibaba.middleware.race.rocketmq.Producer
