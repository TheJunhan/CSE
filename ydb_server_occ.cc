#include "ydb_server_occ.h"
#include "extent_client.h"

// #define DEBUG

//#define DEBUG 1
//tools
bool inline ydb_server_occ::checkId(ydb_protocol::transaction_id id)
{
	if(tramap.find(id) == tramap.end()) return false;
	return true;
}


ydb_server_occ::ydb_server_occ(std::string extent_dst, std::string lock_dst) : ydb_server(extent_dst, lock_dst) {
	//初始化版本号
	for(unsigned long long i = 0; i < 1024; ++i) 
	{
		versions.insert({i, 0});
	}
	//初始化transaction_id
	current_id = 0;
	//初始化锁
	pthread_mutex_init(&ydb_occ_mutex, NULL);
}

ydb_server_occ::~ydb_server_occ() {
}


ydb_protocol::status ydb_server_occ::transaction_begin(int, ydb_protocol::transaction_id &out_id) {    // the first arg is not used, it is just a hack to the rpc lib
	// lab3: your code here
	pthread_mutex_lock(&ydb_occ_mutex);
	out_id = current_id;
	OccTraEntry occTraEntry;
	tramap.insert({out_id, occTraEntry});
	current_id++;
	pthread_mutex_unlock(&ydb_occ_mutex);
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_occ::transaction_commit(ydb_protocol::transaction_id id, int &) {
	// lab3: your code here
	pthread_mutex_lock(&ydb_occ_mutex);
	//检查是否有版本号不一致
	OccTraMap::iterator occTraMap = tramap.find(id);
	for(OccTraEntry::iterator occTraEntry = occTraMap->second.begin(); occTraEntry != occTraMap->second.end(); ++occTraEntry)
	{
		if(occTraEntry->second.version != versions[occTraEntry->first]) 
		{
			// abort_release(id);
			tramap.erase(id);
			pthread_mutex_unlock(&ydb_occ_mutex);
			return ydb_protocol::ABORT;
		}
	}
	//全都一致
	for(OccTraEntry::iterator occTraEntry = occTraMap->second.begin(); occTraEntry != occTraMap->second.end(); ++occTraEntry)
	{
		//版本号变化
		versions[occTraEntry->first]++;
		if(occTraEntry->second.op == 2) ec->put(occTraEntry->first, occTraEntry->second.content);
		else if(occTraEntry->second.op == 3) ec->remove(occTraEntry->first);
	}
	tramap.erase(id);
	pthread_mutex_unlock(&ydb_occ_mutex);
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_occ::transaction_abort(ydb_protocol::transaction_id id, int &) {
	// lab3: your code here
	tramap.erase(id);
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_occ::get(ydb_protocol::transaction_id id, const std::string key, std::string &out_value) {
	// lab3: your code here
	//check
	if(!checkId(id)) return ydb_protocol::TRANSIDINV;
	//哈希
	unsigned long long has = xjh_hash(key);
	//首先检查是否之前读过或者写过
	OccTraMap::iterator current_tra = tramap.find(id);
	OccTraEntry::iterator dstVaid = current_tra->second.find(has);
	if(dstVaid != current_tra->second.end()) 
	{
		out_value = dstVaid->second.content;
#ifdef DEBUG
	cout << "进程" << id << "调用了get函数" << has << " " << out_value << endl;
#endif
		return ydb_protocol::OK;
	}
	//如果之前没有过
	opt tmpopt;
	pthread_mutex_lock(&ydb_occ_mutex);
	ec->get(has, out_value);
	tmpopt.version = versions[has];
#ifdef DEBUG
	cout << "进程" << id << "调用了get函数" << has << " " << out_value << endl;
#endif
	pthread_mutex_unlock(&ydb_occ_mutex);
	tmpopt.op = 1;
	tmpopt.content = out_value;
	current_tra->second.insert({has, tmpopt});
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_occ::set(ydb_protocol::transaction_id id, const std::string key, const std::string value, int &) {
	// lab3: your code here
	//check
	if(!checkId(id)) return ydb_protocol::TRANSIDINV;
	//哈希
	unsigned long long has = xjh_hash(key);
	//首先检查是否之前读过或者写过
	OccTraMap::iterator current_tra = tramap.find(id);
	OccTraEntry::iterator dstVaid = current_tra->second.find(has);
	if(dstVaid != current_tra->second.end()) 
	{
		dstVaid->second.content = value;
		dstVaid->second.op = 2;
#ifdef DEBUG
	cout << "进程" << id << "调用了set函数" << has << " " << value << endl;
#endif
		return ydb_protocol::OK;
	}
	//如果之前没有过
	opt tmpopt;
	pthread_mutex_lock(&ydb_occ_mutex);
	tmpopt.version = versions[has];
#ifdef DEBUG
	cout << "进程" << id << "调用了set函数" << has << " " << value << endl;
#endif
	pthread_mutex_unlock(&ydb_occ_mutex);
	tmpopt.content = value;
	tmpopt.op = 2;
	current_tra->second.insert({has, tmpopt});
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_occ::del(ydb_protocol::transaction_id id, const std::string key, int &) {
	// lab3: your code here
	//check
	if(!checkId(id)) return ydb_protocol::TRANSIDINV;
	//哈希
	unsigned long long has = xjh_hash(key);
	//首先检查是否之前读过或者写过
	OccTraMap::iterator current_tra = tramap.find(id);
	OccTraEntry::iterator dstVaid = current_tra->second.find(has);
	if(dstVaid != current_tra->second.end()) 
	{
		dstVaid->second.content = "";
		dstVaid->second.op = 3;
#ifdef DEBUG
	cout << "进程" << id << "调用了del函数" << has << endl;
#endif
		return ydb_protocol::OK;
	}
	//如果之前没有过
	opt tmpopt;
	pthread_mutex_lock(&ydb_occ_mutex);
	tmpopt.version = versions[has];
#ifdef DEBUG
	cout << "进程" << id << "调用了del函数" << has << endl;
#endif
	pthread_mutex_unlock(&ydb_occ_mutex);
	tmpopt.content = "";
	tmpopt.op = 3;
	current_tra->second.insert({has, tmpopt});
	return ydb_protocol::OK;
}

