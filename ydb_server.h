#ifndef ydb_server_h
#define ydb_server_h

#include <string>
#include <map>
#include <vector>
#include <set>
#include "extent_client.h"
#include "lock_protocol.h"
#include "lock_client.h"
#include "lock_client_cache.h"
#include "ydb_protocol.h"
#include "extent_cache_client.h"


class ydb_server {
protected:
	extent_cache_client *ec;
	// extent_client *ec;
	lock_client *lc;
	// lock_client_cache *lc;
public:
	ydb_server(std::string, std::string);
	virtual ~ydb_server();
	virtual ydb_protocol::status transaction_begin(int, ydb_protocol::transaction_id &);
	virtual ydb_protocol::status transaction_commit(ydb_protocol::transaction_id, int &);
	virtual ydb_protocol::status transaction_abort(ydb_protocol::transaction_id, int &);
	virtual ydb_protocol::status get(ydb_protocol::transaction_id, const std::string, std::string &);
	virtual ydb_protocol::status set(ydb_protocol::transaction_id, const std::string, const std::string, int &);
	virtual ydb_protocol::status del(ydb_protocol::transaction_id, const std::string, int &);
	unsigned long long xjh_hash(const std::string key);
};

#endif

