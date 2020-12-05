#ifndef ydb_server_2pl_h
#define ydb_server_2pl_h

#include <string>
#include <map>
#include <vector>
#include "extent_client.h"
#include "lock_protocol.h"
#include "lock_client.h"
#include "lock_client_cache.h"
#include "ydb_protocol.h"
#include "ydb_server.h"
#include "rpc.h"
#include <pthread.h>

using namespace std;

struct opt
	{
		ydb_protocol::transaction_id id;
		//begin = 0, get = 1, set = 2, del = 3
		// optname op;
		int op;
		string content;
		unsigned long long vaid;
		bool waiting;
		opt(ydb_protocol::transaction_id iid, int oopt)
		{
			id = iid;
			op = oopt;
		}
	};

class ydb_server_2pl: public ydb_server {
private:
	vector<vector<opt> > tra;
	ydb_protocol::transaction_id current_id;

	static pthread_mutex_t ydb_mutex;
public:
	ydb_server_2pl(std::string, std::string);
	~ydb_server_2pl();
	void abort_release(vector<vector<opt> >::iterator i);
	vector<vector<opt> >::iterator find(ydb_protocol::transaction_id id);
	bool judge(vector<vector<opt> >::iterator dst);
	vector<vector<opt> >::iterator depend(vector<vector<opt> >::iterator originorigin, vector<vector<opt> >::iterator origin);
	bool check_circle(vector<vector<opt> >::iterator origin, vector<vector<opt> >::iterator last, vector<vector<opt> >::iterator current);
	// void addopt(optname opt, unsigned long long vaid, string content);
	ydb_protocol::status transaction_begin(int, ydb_protocol::transaction_id &);
	ydb_protocol::status transaction_commit(ydb_protocol::transaction_id, int &);
	ydb_protocol::status transaction_abort(ydb_protocol::transaction_id, int &);
	ydb_protocol::status get(ydb_protocol::transaction_id, const std::string, std::string &);
	ydb_protocol::status set(ydb_protocol::transaction_id, const std::string, const std::string, int &);
	ydb_protocol::status del(ydb_protocol::transaction_id, const std::string, int &);
	void prin();

	// enum optname
	// {
	// 	begin = 0,
	// 	commit,
	// 	get,
	// 	set,
	// 	del
	// };

};

#endif

