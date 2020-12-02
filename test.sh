# 这个是测试durability
# ./start_ydb.sh 2PL
# ./test-lab3-durability.sh 2PL
# ./stop_ydb.sh

# 这个是测试basic
./start_ydb.sh 2PL
./test-lab3-part2-3-basic 4772
./stop_ydb.sh

# 这个是2pl无死锁
# ./start_ydb.sh 2PL
# ./test-lab3-part2-a 4772
# ./stop_ydb.sh
