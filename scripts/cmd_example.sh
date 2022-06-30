./build/chakra-cli --command=state --ip=10.210.16.240 --port=7290
./build/chakra-cli --command=meet --ip=10.210.16.240 --port=7290 --meet_ip=10.210.16.240 --meet_port=9291
./build/chakra-cli --command=setdb --ip=10.210.16.240 --port=7290  --setdb_name=db1 --setdb_nodename=1656041923728-ULYMBRCFRVRVLICQXWLERABWYH --setdb_cached=1000000
./build/chakra-cli --command=set --ip=10.210.16.240 --port=7290  --set_keys=db1:key_1 --set_values=value_1 --push_ttl=2000
./build/chakra-cli --command=push --ip=10.210.16.240 --port=7290  --push_key=db1:key_1 --push_values=aaa,bbb,ccc --push_ttl=2000
./build/chakra-cli --command=get --ip=10.210.16.240 --port=7290  --get_key=db1:my_key_1 --hashed
./build/chakra-cli --command=mget --ip=10.210.16.240 --port=7290  --mget_keys=db1:bmcs5_key_0,db1:bmcs5_key_1,db1:bmcs5_key_2

docker run -dit --privileged --restart unless-stopped --name wangzk_chakra1 -v /data/devimagespace/wangzk_chakra1:/workspace -v /data/src:/data/src -v /data/sdks:/sdks --network host hub.bilibili.co/compile/chakra:v1.0.0 bash
docker run -dit --privileged --restart unless-stopped --name wangzk_chakra2 -v /data/devimagespace/wangzk_chakra2:/workspace -v /data/src:/data/src -v /data/sdks:/sdks --network host hub.bilibili.co/compile/chakra:v1.0.0 bash
docker run -dit --privileged --restart unless-stopped --name wangzk_chakra3 -v /data/devimagespace/wangzk_chakra3:/workspace -v /data/src:/data/src -v /data/sdks:/sdks --network host hub.bilibili.co/compile/chakra:v1.0.0 bash
docker run -dit --privileged --restart unless-stopped --name wangzk_chakra4 -v /data/devimagespace/wangzk_chakra4:/workspace -v /data/src:/data/src -v /data/sdks:/sdks --network host hub.bilibili.co/compile/chakra:v1.0.0 bash
echo "core-%e-%p-%t" > /proc/sys/kernel/core_pattern
nohup ./chakra --flagfile=./config/chakra.conf > a.log 2>&1 &


