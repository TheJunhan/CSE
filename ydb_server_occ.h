#ifndef ydb_server_occ_h
#define ydb_server_occ_h

#include <string>
#include <map>
#include "extent_client.h"
#include "lock_protocol.h"
#include "lock_client.h"
#include "lock_client_cache.h"
#include "ydb_protocol.h"
#include "ydb_server.h"

using namespace std;

struct opt
{
	//get for 1, set for 2, del for 3
	int op;
	int version;
	string content;
	opt()
	{
		content = "";
	}
};

typedef map<unsigned long long, opt> OccTraEntry;
typedef map<int, OccTraEntry> OccTraMap;

class ydb_server_occ: public ydb_server {
private:
	int current_id;
	OccTraMap tramap;
	map<unsigned long long, int> versions;
	pthread_mutex_t ydb_occ_mutex;
public:
	ydb_server_occ(std::string, std::string);
	~ydb_server_occ();
	void abort_release(int id);
	bool inline checkId(ydb_protocol::transaction_id id);
	ydb_protocol::status transaction_begin(int, ydb_protocol::transaction_id &);
	ydb_protocol::status transaction_commit(ydb_protocol::transaction_id, int &);
	ydb_protocol::status transaction_abort(ydb_protocol::transaction_id, int &);
	ydb_protocol::status get(ydb_protocol::transaction_id, const std::string, std::string &);
	ydb_protocol::status set(ydb_protocol::transaction_id, const std::string, const std::string, int &);
	ydb_protocol::status del(ydb_protocol::transaction_id, const std::string, int &);
};

#endif

