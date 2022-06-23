./build/chakra-cli --command=get --ip=127.0.0.1 --port=7290  --get_key=db1:key_1
./build/chakra-cli --command=mget --ip=127.0.0.1 --port=7290  --mget_keys=db1:key_1,db1:key_2
./build/chakra-cli --command=set --ip=127.0.0.1 --port=7290  --set_keys=db1:key_1 --set_values=value_1 --push_ttl=2000
./build/chakra-cli --command=push --ip=127.0.0.1 --port=7290  --push_key=db1:key_1 --push_values=aaa,bbb,ccc --push_ttl=2000
./build/chakra-cli --command=setdb --ip=127.0.0.1 --port=7290  --setdb_name=db1 --setdb_nodename=1652797583029-DHGIRAHGQSQDMRBACNKTLVQXZY --setdb_cached=1000000
./build/chakra-cli --command=state --ip=127.0.0.1 --port=7290
./build/chakra-cli --command=meet --ip=127.0.0.1 --port=7290 --meet_ip=10.210.16.240 --meet_port=8291


docker run -dit --restart unless-stopped --name wangzk_chakra1 -v /data/devimagespace/wangzk_chakra1:/workspace -v /data/src:/data/src -v /data/sdks:/sdks --network host hub.bilibili.co/compile/chakra:v1.0.0 bash
docker run -dit --restart unless-stopped --name wangzk_chakra2 -v /data/devimagespace/wangzk_chakra2:/workspace -v /data/src:/data/src -v /data/sdks:/sdks --network host hub.bilibili.co/compile/chakra:v1.0.0 bash
docker run -dit --restart unless-stopped --name wangzk_chakra3 -v /data/devimagespace/wangzk_chakra3:/workspace -v /data/src:/data/src -v /data/sdks:/sdks --network host hub.bilibili.co/compile/chakra:v1.0.0 bash
nohup ./chakra --flagfile=./config/chakra.conf > a.log 2>&1 &