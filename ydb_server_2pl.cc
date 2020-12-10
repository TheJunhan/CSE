#include "ydb_server_2pl.h"
#include "extent_client.h"
#include <iostream>

using namespace std;

pthread_mutex_t ydb_server_2pl::ydb_mutex = PTHREAD_MUTEX_INITIALIZER;
//#define DEBUG 1

//tools
bool inline ydb_server_2pl::checkId(ydb_protocol::transaction_id id)
{
	if(tra.find(id) == tra.end()) return false;
	return true;
}

void ydb_server_2pl::abort_release(traEntry tmp)
{
	for(traEntry::iterator i = tmp.begin(); i != tmp.end(); ++i)
	{
		if(!i->second.waiting)
			{
				locks[i->first] = 0;
				pthread_cond_signal(&ydb_cond[i->first]);
			}
	}
}

traMap global_tool_tra;

void global_prin()
{
	for(traMap::iterator i = global_tool_tra.begin(); i != global_tool_tra.end(); ++i)
    {	
	    for(traEntry::iterator j = i->second.begin(); j != i->second.end(); ++j)
			cout << j->first << " " << j->second.waiting << ", ";
		cout << endl;
	}
}

unsigned long long ydb_server_2pl::findDepend(unsigned long long current)
{
	global_tool_tra = tra;
	unsigned long long dstHas;
	traMap::iterator tmp = global_tool_tra.find(current);
	//找到要依赖的元素
	for(traEntry::iterator i = tmp->second.begin(); i != tmp->second.end(); ++i) 
		if(i->second.waiting) 
		{
			dstHas = i->first;
			break;
		}
	for(traMap::iterator i = global_tool_tra.begin(); i != global_tool_tra.end(); ++i)
	{
		if(i == tmp) continue;
		if(!i->second[dstHas].waiting) 
		{
			return i->first;
		}
	}
	return -1;
}

bool ydb_server_2pl::checkDead(unsigned long long origin, unsigned long long current)
{
	//找到依赖
	global_tool_tra = tra;
	unsigned long long next = findDepend(current);
	if(next == -1) return false;
	if(next == origin) return true;
	return checkDead(origin, next);
}

void ydb_server_2pl::prin()
{
	for(traMap::iterator i = tra.begin(); i != tra.end(); ++i)
    {	
	    for(traEntry::iterator j = i->second.begin(); j != i->second.end(); ++j)
			cout << j->first << " " << j->second.waiting << ", ";
		cout << endl;
	}
}

ydb_server_2pl::ydb_server_2pl(std::string extent_dst, std::string lock_dst) : ydb_server(extent_dst, lock_dst) {
	current_id = 0;
	for(unsigned long long i = 0; i <= 1024; ++i) 
	{
		locks.insert({i, 0});
		pthread_cond_init(&ydb_cond[i], NULL);
	}
}

ydb_server_2pl::~ydb_server_2pl() {
}

ydb_protocol::status ydb_server_2pl::transaction_begin(int, ydb_protocol::transaction_id &out_id) {    // the first arg is not used, it is just a hack to the rpc lib
	// lab3: your code here
	pthread_mutex_lock(&ydb_mutex);
	traEntry newTra;
	tra.insert({current_id, newTra});
	out_id = current_id;
	current_id++;
	pthread_mutex_unlock(&ydb_mutex);
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_2pl::transaction_commit(ydb_protocol::transaction_id id, int &) {
	// lab3: your code here
	if(!checkId(id)) return ydb_protocol::TRANSIDINV;
	pthread_mutex_lock(&ydb_mutex);
	traEntry tmpTra = tra[id];
	traEntry::iterator i = tmpTra.begin();
	for(; i != tmpTra.end(); ++i)
	{
		if(i->second.op == 1) continue;
		else if(i->second.op == 2) 
		{
			ec->put(i->first, i->second.content);
		}
		else ec->remove(i->first);
	}
	abort_release(tmpTra);
	tra.erase(id);
	pthread_mutex_unlock(&ydb_mutex);
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_2pl::transaction_abort(ydb_protocol::transaction_id id, int &) {
	// lab3: your code here
	if(!checkId(id)) return ydb_protocol::TRANSIDINV;
	pthread_mutex_lock(&ydb_mutex);
	traEntry tmp = tra[id];
	abort_release(tmp);
	tra.erase(id);
	pthread_mutex_unlock(&ydb_mutex);
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_2pl::get(ydb_protocol::transaction_id id, const std::string key, std::string &out_value) {
	// lab3: your code here
	if(!checkId(id)) return ydb_protocol::TRANSIDINV;
	pthread_mutex_lock(&ydb_mutex);
	//哈希
	unsigned long long has = xjh_hash(key);
	//首先检查是否有set或者get过
	traEntry tmpTra = tra[id];
	traEntry::iterator i = tmpTra.find(has);
	if(i != tmpTra.end()) 
	{
		out_value = i->second.content;
		pthread_mutex_unlock(&ydb_mutex);
		return ydb_protocol::OK;
	}
	//检查死锁
	ydb_2pl_stat sta;
	sta.op = 1;
	tmpTra.insert({has, sta});
	tra[id] = tmpTra;
	global_tool_tra = tra;
	if(checkDead(id, id)) 
	{
		abort_release(tmpTra);
		tra.erase(id);
		pthread_mutex_unlock(&ydb_mutex);
		return ydb_protocol::ABORT;
	}
	//获取锁
	while(locks[has])
	{
		pthread_cond_wait(&ydb_cond[has], &ydb_mutex);
	}
	locks[has] = 1;
	ec->get(has, out_value);
	sta.waiting = 0;
	sta.content = out_value;
	tmpTra[has] = sta;
	tra[id] = tmpTra;
	pthread_mutex_unlock(&ydb_mutex);
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_2pl::set(ydb_protocol::transaction_id id, const std::string key, const std::string value, int &) {
	// lab3: your code here
	if(!checkId(id)) return ydb_protocol::TRANSIDINV;
	pthread_mutex_lock(&ydb_mutex);
	//哈希
	unsigned long long has = xjh_hash(key);
	//检查自己是否之前有set或者get
	traEntry tmpTra = tra[id];
	traEntry::iterator i = tmpTra.find(has);
	if(i != tmpTra.end())
	{
		i->second.op = 2;
		i->second.content = value;
		tra[id] = tmpTra;
		pthread_mutex_unlock(&ydb_mutex);
		return ydb_protocol::OK;
	}
	//自己动手，别忘了加上死锁检查
	ydb_2pl_stat sta;
	sta.content = value;
	sta.op = 2;
	tra[id][has] = sta;
	cout << id << endl;
	global_tool_tra = tra;
	traMap::iterator checkTmp = global_tool_tra.find(id);
	if(checkDead(id, id)) 
	{
		abort_release(tmpTra);
		tra.erase(id);
		pthread_mutex_unlock(&ydb_mutex);
		return ydb_protocol::ABORT;
	}
	while(locks[has])
		pthread_cond_wait(&ydb_cond[has], &ydb_mutex);
	locks[has] = 1;
	sta.waiting = 0;
	tra[id][has] = sta;
	cout << id << endl;
	pthread_mutex_unlock(&ydb_mutex);
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_2pl::del(ydb_protocol::transaction_id id, const std::string key, int &) {
	// lab3: your code here
	if(!checkId(id)) return ydb_protocol::TRANSIDINV;
	pthread_mutex_lock(&ydb_mutex);
	//哈希
	unsigned long long has = xjh_hash(key);
	//检查自己之前是否有过对这个元素的操作
	traEntry tmpTra = tra[id];
	traEntry::iterator i = tmpTra.find(has);
	if(i != tmpTra.end())
	{
		i->second.op = 3;
		i->second.content = "";
		tra[id] = tmpTra;
		pthread_mutex_unlock(&ydb_mutex);
	}
	//自己动手
	ydb_2pl_stat sta;
	sta.op = 3;
	tmpTra.insert({has, sta});
	tra[id] = tmpTra;
	global_tool_tra = tra;
	traMap::iterator checkTmp = global_tool_tra.find(id);
	if(checkDead(id, id)) 
	{
		abort_release(tmpTra);
		tra.erase(id);
		pthread_mutex_unlock(&ydb_mutex);
		return ydb_protocol::ABORT;
	}
	while(locks[has])
		pthread_cond_wait(&ydb_cond[has], &ydb_mutex);
	locks[has] = 1;
	sta.waiting = 0;
	tmpTra[has] = sta;
	tra[id] = tmpTra;

	pthread_mutex_unlock(&ydb_mutex);
	return ydb_protocol::OK;
}

