#ifndef ydb_server_2pl_h
#define ydb_server_2pl_h

#include <string>
#include <map>
#include "extent_client.h"
#include "lock_protocol.h"
#include "lock_client.h"
#include "lock_client_cache.h"
#include "ydb_protocol.h"
#include "ydb_server.h"
#include "rpc.h"
#include <pthread.h>

using namespace std;

struct ydb_2pl_stat
{
	//begin = 0, get = 1, set = 2, del = 3
	// optname op;
	int op;
	string content;
	bool waiting;
	ydb_2pl_stat()
	{
		waiting = 1;
		content = "";
	}
};

typedef map<unsigned long long, ydb_2pl_stat> traEntry;
typedef map<ydb_protocol::transaction_id, traEntry> traMap;
typedef map<unsigned long long, bool> lockMap;

class ydb_server_2pl: public ydb_server {
private:
	traMap tra;
	lockMap locks;
	static pthread_mutex_t ydb_mutex;
	static pthread_cond_t ydb_cond;
	int current_id;
public:
	ydb_server_2pl(std::string, std::string);
	~ydb_server_2pl();
	traEntry::iterator inline findEntry(traEntry tmp, unsigned long long has);
	//找死锁的入口
	bool checkDead(unsigned long long i, unsigned long long i2);
	//找到依赖
	unsigned long long findDepend(unsigned long long current);
	//输出map
	void prin();
	void abort_release(traEntry tmp);
	bool inline checkId(ydb_protocol::transaction_id id);
	ydb_protocol::status transaction_begin(int, ydb_protocol::transaction_id &);
	ydb_protocol::status transaction_commit(ydb_protocol::transaction_id, int &);
	ydb_protocol::status transaction_abort(ydb_protocol::transaction_id, int &);
	ydb_protocol::status get(ydb_protocol::transaction_id, const std::string, std::string &);
	ydb_protocol::status set(ydb_protocol::transaction_id, const std::string, const std::string, int &);
	ydb_protocol::status del(ydb_protocol::transaction_id, const std::string, int &);
};

#endif

