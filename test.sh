# 2PL ---------------------------------------

# 这个是测试durability
# ./start_ydb.sh 2PL
# ./test-lab3-durability.sh 2PL
# ./stop_ydb.sh

# 这个是测试basic
# ./start_ydb.sh 2PL
# ./test-lab3-part2-3-basic 4772
# ./stop_ydb.sh

# 这个是2pl无死锁
# ./start_ydb.sh 2PL
# ./test-lab3-part2-a 4772
# ./stop_ydb.sh

# 这个是2pl有死锁
# ./start_ydb.sh 2PL
# ./test-lab3-part2-b 4772
# ./stop_ydb.sh

#complex
# ./start_ydb.sh 2PL
# ./test-lab3-part2-3-complex 4772
# ./stop_ydb.sh

# OCC ---------------------------------------

# 这个是测试durability
# ./start_ydb.sh OCC
# ./test-lab3-durability.sh OCC
# ./stop_ydb.sh

# # 这个是测试basic
# ./start_ydb.sh OCC
# ./test-lab3-part2-3-basic 4772
# ./stop_ydb.sh

# # 无conflict
# ./start_ydb.sh OCC
# ./test-lab3-part3-a 4772
# ./stop_ydb.sh

# # 有conflict
# ./start_ydb.sh OCC
# ./test-lab3-part3-b 4772
# ./stop_ydb.sh

# complex
./start_ydb.sh OCC
./test-lab3-part2-3-complex 4772
./stop_ydb.sh