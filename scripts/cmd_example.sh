./build/chakra-cli --ip=127.0.0.1 --port=7290 --command=mget --mget_keys=db1:key_1,db1:key_2
./build/chakra-cli --ip=127.0.0.1 --port=7290 --command=get --get_key=db1:key_1
./build/chakra-cli --ip=127.0.0.1 --port=7290 --command=push --push_key=key_1 --push_values=aaa,bbb,ccc --push_ttl=2000
./build/chakra-cli --ip=127.0.0.1 --port=7290 --command=setdb --setdb_name=db1 --setdb_nodename=1652797583029-DHGIRAHGQSQDMRBACNKTLVQXZY --setdb_cached=1000000
./build/chakra-cli --ip=127.0.0.1 --port=7290 --command=state
./build/chakra-cli --command=meet --ip=127.0.0.1 --port=7290 --meet_ip=172.18.0.4 --meet_port=7291


docker run -dit --restart unless-stopped --name wangzk_chakra1 -v /data/devimagespace/wangzk_chakra1:/workspace -v /data/src:/data/src -v /data/sdks:/sdks -p 7290:7290 -p 7291:7291 --network chakra-net hub.bilibili.co/compile/chakra:v1.0.0 bash
docker run -dit --restart unless-stopped --name wangzk_chakra2 -v /data/devimagespace/wangzk_chakra2:/workspace -v /data/src:/data/src -v /data/sdks:/sdks -p 8290:7290 -p 8291:7291 --network chakra-net hub.bilibili.co/compile/chakra:v1.0.0 bash
docker run -dit --restart unless-stopped --name wangzk_chakra3 -v /data/devimagespace/wangzk_chakra3:/workspace -v /data/src:/data/src -v /data/sdks:/sdks -p 9290:7290 -p 9291:7291 --network chakra-net hub.bilibili.co/compile/chakra:v1.0.0 bash

docker run -dit --restart unless-stopped --name wangzk_chakra4 -v /data/devimagespace/wangzk_chakra4:/workspace -v /data/src:/data/src -v /data/sdks:/sdks -p 7290:7290 -p 7291:7291 hub.bilibili.co/compile/chakra:v1.0.0 bash
cd /code/chakra
nohup ./chakra --flagfile=./config/chakra.conf > a.log 2>&1 &
./chakra-cli --command=state --ip=10.210.16.240 --port=7290 
--meet_ip=172.18.0.4 --meet_port=7291