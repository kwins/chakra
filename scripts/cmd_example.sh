./build/chakra-cli --ip=127.0.0.1 --port=7290 --command=mget --mget_keys=db1:key_1,db1:key_2
./build/chakra-cli --ip=127.0.0.1 --port=7290 --command=get --get_key=db1:key_1
./build/chakra-cli --ip=127.0.0.1 --port=7290 --command=push --push_key=key_1 --push_values=aaa,bbb,ccc --push_ttl=2000
./build/chakra-cli --ip=127.0.0.1 --port=7290 --command=setdb --setdb_name=db1 --setdb_nodename=1652797583029-DHGIRAHGQSQDMRBACNKTLVQXZY --setdb_cached=1000000
./build/chakra-cli --ip=127.0.0.1 --port=7290 --command=state