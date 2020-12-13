#include "ydb_server.h"
#include "extent_client.h"
#include <iostream>

using namespace std;

//#define DEBUG 1

static long timestamp(void) {
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return (tv.tv_sec*1000 + tv.tv_usec/1000);
}

ydb_server::ydb_server(std::string extent_dst, std::string lock_dst) {
	// ec = new extent_client(extent_dst);
	ec = new extent_cache_client(extent_dst);
	lc = new lock_client(lock_dst);
	// lc = new lock_client_cache(lock_dst);
	long starttime = timestamp();
	
	for(int i = 2; i < 1024; i++) {    // for simplicity, just pre alloc all the needed inodes
		extent_protocol::extentid_t id;
		ec->create(extent_protocol::T_FILE, id);
	}
	
	long endtime = timestamp();
}

ydb_server::~ydb_server() {
	delete lc;
	delete ec;
}

//tools

unsigned long long ydb_server::xjh_hash(const std::string key)
{
	unsigned long long res = 0;
	for(int i = 0; i < key.size(); ++i)
	{
		if(key.at(i) <= '9' && key.at(i) >= '0')
			res += ((unsigned long long)key.at(i)) * 13 * (i + 1);
		else
			res += (unsigned long long)key.at(i) * (i + 1);
	}
	return (res % 1024);
}

ydb_protocol::status ydb_server::transaction_begin(int, ydb_protocol::transaction_id &out_id) {    // the first arg is not used, it is just a hack to the rpc lib
	// no imply, just return OK
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server::transaction_commit(ydb_protocol::transaction_id id, int &) {
	// no imply, just return OK
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server::transaction_abort(ydb_protocol::transaction_id id, int &) {
	// no imply, just return OK
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server::get(ydb_protocol::transaction_id id, const std::string key, std::string &out_value) {
	// lab3: your code here
	extent_protocol::attr a;
	if(ec->getattr(xjh_hash(key), a) != extent_protocol::OK) 
	{
		return ydb_protocol::RPCERR;
	}
	ec->get(xjh_hash(key), out_value);
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server::set(ydb_protocol::transaction_id id, const std::string key, const std::string value, int &) {
	// lab3: your code here
	extent_protocol::attr a;
	if(ec->getattr(xjh_hash(key), a) != extent_protocol::OK) 
	{
		return ydb_protocol::RPCERR;
	}
	ec->put(xjh_hash(key), value);
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server::del(ydb_protocol::transaction_id id, const std::string key, int &) {
	// lab3: your code here
	extent_protocol::attr a;
	if(ec->getattr(xjh_hash(key), a) != extent_protocol::OK) 
	{
		return ydb_protocol::RPCERR;
	}
	ec->remove(xjh_hash(key));
	return ydb_protocol::OK;
}

