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
	// cout << "ydbserver初始化开始" << endl;
	// cout << extent_dst << endl;
	ec = new extent_client(extent_dst);
	// cout << "ydbserver结束" << endl;
	lc = new lock_client(lock_dst);
	// lc = new lock_client_cache(lock_dst);
	long starttime = timestamp();
	
	for(int i = 2; i < 1024; i++) {    // for simplicity, just pre alloc all the needed inodes
		extent_protocol::extentid_t id;
		ec->create(extent_protocol::T_FILE, id);
	}
	
	long endtime = timestamp();
	// printf("time %ld ms\n", endtime-starttime);
}

ydb_server::~ydb_server() {
	delete lc;
	delete ec;
}

//tools

unsigned long long  ydb_server::xjh_hash(const std::string key)
{
	unsigned long long res = 0;
	for(int i = 0; i < key.size(); ++i)
	{
		res += (unsigned long long)key.at(i);
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
		// printf("wrong in ydb get\n");
		return ydb_protocol::RPCERR;
	}
	ec->get(xjh_hash(key), out_value);
	cout << "调用了get其中key为：" << xjh_hash(key) << "得到的value为：";
	cout << out_value << endl;
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server::set(ydb_protocol::transaction_id id, const std::string key, const std::string value, int &) {
	// lab3: your code here
	extent_protocol::attr a;
	if(ec->getattr(xjh_hash(key), a) != extent_protocol::OK) 
	{
		// printf("wrong in ydb set\n");
		return ydb_protocol::RPCERR;
	}
	ec->put(xjh_hash(key), value);
	cout << "调用了set，其中key为：" << xjh_hash(key) << "value为：" << value << endl;
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server::del(ydb_protocol::transaction_id id, const std::string key, int &) {
	// lab3: your code here
	cout << "调用了del，要删的元素key为：" << endl;
	extent_protocol::attr a;
	if(ec->getattr(xjh_hash(key), a) != extent_protocol::OK) 
	{
		// printf("wrong in ydb del\n");
		return ydb_protocol::RPCERR;
	}
	ec->remove(xjh_hash(key));
	return ydb_protocol::OK;
}

