#include "ydb_server_2pl.h"
#include "extent_client.h"
#include <iostream>

using namespace std;

pthread_mutex_t ydb_server_2pl::ydb_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t ydb_server_2pl::ydb_cond = PTHREAD_COND_INITIALIZER;
//#define DEBUG 1

//tools
// traEntry::iterator inline ydb_server_2pl::findEntry(traEntry tmp, unsigned long long has)
// {
// 	return tmp.find(has);
// }
bool inline ydb_server_2pl::checkId(ydb_protocol::transaction_id id)
{
	// cout << "想访问" << id << endl;
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
				cout << "abort_release解锁了" << i->first << endl;
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
	unsigned long long dstHas;
	traMap::iterator tmp = global_tool_tra.find(current);
	//找到要依赖的元素
	for(traEntry::iterator i = tmp->second.begin(); i != tmp->second.end(); ++i) 
		if(i->second.waiting) 
		{
			cout << "找到了正在waiting的" << i->first << endl;
			dstHas = i->first;
			break;
		}
	for(traMap::iterator i = global_tool_tra.begin(); i != global_tool_tra.end(); ++i)
	{
		if(i == tmp) continue;
		if(!i->second[dstHas].waiting) 
		{
			cout << "找依赖函数将要返回：" << i->first << endl;
			return i->first;
		}
	}
	cout << "找依赖函数将要返回空值" << endl;
	return -1;
}

// bool ydb_server_2pl::checkDead(traMap::iterator origin, traMap::iterator current)
bool ydb_server_2pl::checkDead(unsigned long long origin, unsigned long long current)
{
	// //找到waiting的元素
	// unsigned long long dstHas = findWaitingHas(current);
	//找到依赖
	cout << "将要对" << current << "进行找依赖" << endl;
	global_prin();
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
	}
}

ydb_server_2pl::~ydb_server_2pl() {
}

ydb_protocol::status ydb_server_2pl::transaction_begin(int, ydb_protocol::transaction_id &out_id) {    // the first arg is not used, it is just a hack to the rpc lib
	// lab3: your code here
	pthread_mutex_lock(&ydb_mutex);
	traEntry newTra;
	// cout << 1 << endl;
	tra.insert({current_id, newTra});
	out_id = current_id;
	// cout << "开始了transaction" << current_id << endl;
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
			// cout << "commit 的时候put了" << i->first << "为" << i->second.content << endl;
		}
		else ec->remove(i->first);
	}
	abort_release(tmpTra);
	tra.erase(id);
	pthread_cond_signal(&ydb_cond);
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
	// cout << "调用了get"<< has << endl;
	//首先检查是否有set或者get过
	traEntry tmpTra = tra[id];
	traEntry::iterator i = tmpTra.find(has);
	if(i != tmpTra.end()) 
	{
		out_value = i->second.content;
		cout << "get获得了" << has << "为" << out_value << endl;
		pthread_mutex_unlock(&ydb_mutex);
		return ydb_protocol::OK;
	}
	//检查死锁
	ydb_2pl_stat sta;
	sta.op = 1;
	tmpTra.insert({has, sta});
	tra[id] = tmpTra;
	cout << "get"<< has << "改变了map" << endl;
	prin();
	global_tool_tra = tra;
	// traMap::iterator checkTmp = global_tool_tra.find(id);
	// if(checkDead(checkTmp, checkTmp)) 
	if(checkDead(id, id)) 
	{
		// pthread_mutex_lock(&ydb_mutex);
		abort_release(tmpTra);
		tra.erase(id);
		pthread_cond_signal(&ydb_cond);
		pthread_mutex_unlock(&ydb_mutex);
		return ydb_protocol::ABORT;
	}
	//获取锁
	// pthread_mutex_lock(&ydb_mutex);
	while(locks[has])
	{
		pthread_cond_wait(&ydb_cond, &ydb_mutex);
	}
	cout << "也许被唤醒了" << endl;
	locks[has] = 1;
	ec->get(has, out_value);
	sta.waiting = 0;
	sta.content = out_value;
	tmpTra[has] = sta;
	tra[id] = tmpTra;
	cout << "get"<< has << "改变了map" << endl;
	prin();
	// cout << "get获得了" << has << "为" << out_value << endl;
	pthread_mutex_unlock(&ydb_mutex);
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_2pl::set(ydb_protocol::transaction_id id, const std::string key, const std::string value, int &) {
	// lab3: your code here
	if(!checkId(id)) return ydb_protocol::TRANSIDINV;
	pthread_mutex_lock(&ydb_mutex);
	//哈希
	unsigned long long has = xjh_hash(key);
	// cout << "调用了set"<< has << endl;
	//检查自己是否之前有set或者get
	traEntry tmpTra = tra[id];
	traEntry::iterator i = tmpTra.find(has);
	if(i != tmpTra.end())
	{
		cout << "到这里来了" << endl;
		i->second.op = 2;
		i->second.content = value;
		tra[id] = tmpTra;
		// cout << tmpTra.size() << endl;
		// cout << "set改变了" << has << "为" << value << endl;
		pthread_mutex_unlock(&ydb_mutex);
		return ydb_protocol::OK;
	}
	//自己动手，别忘了加上死锁检查
	ydb_2pl_stat sta;
	sta.content = value;
	sta.op = 2;
	// tmpTra.insert({has, sta});
	// tra[id] = tmpTra;
	tra[id][has] = sta;
	cout << id << endl;
	cout << "set"<< has << "改变了map" << endl;
	prin();
	global_tool_tra = tra;
	traMap::iterator checkTmp = global_tool_tra.find(id);
	if(checkDead(id, id)) 
	{
		// pthread_mutex_lock(&ydb_mutex);
		abort_release(tmpTra);
		tra.erase(id);
		cout << "将要完成唤醒函数" << endl;
		pthread_cond_signal(&ydb_cond);
		cout << "完成了唤醒函数" << endl;
		pthread_mutex_unlock(&ydb_mutex);
		return ydb_protocol::ABORT;
	}
	while(locks[has])
		pthread_cond_wait(&ydb_cond, &ydb_mutex);
	cout << "也许被唤醒了" << endl;
	locks[has] = 1;
	sta.waiting = 0;
	// tmpTra[has] = sta;
	// tra[id] = tmpTra;
	tra[id][has] = sta;
	cout << id << endl;
	cout << "set"<< has << "改变了map" << endl;
	prin();
	// cout << "set改变了" << has << "为" << value << endl;
	pthread_mutex_unlock(&ydb_mutex);
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_2pl::del(ydb_protocol::transaction_id id, const std::string key, int &) {
	// lab3: your code here
	if(!checkId(id)) return ydb_protocol::TRANSIDINV;
	pthread_mutex_lock(&ydb_mutex);
	//哈希
	unsigned long long has = xjh_hash(key);
	// cout << "调用了del"<< has << endl;
	//检查自己之前是否有过对这个元素的操作
	traEntry tmpTra = tra[id];
	traEntry::iterator i = tmpTra.find(has);
	if(i != tmpTra.end())
	{
		// pthread_mutex_lock(&ydb_mutex);
		i->second.op = 3;
		i->second.content = "";
		tra[id] = tmpTra;
		// cout << "del删除了" << has << endl;
		pthread_mutex_unlock(&ydb_mutex);
	}
	//自己动手
	// pthread_mutex_lock(&ydb_mutex);
	ydb_2pl_stat sta;
	sta.op = 3;
	tmpTra.insert({has, sta});
	tra[id] = tmpTra;
	cout << "del"<< has << "改变了map" << endl;
	prin();
	global_tool_tra = tra;
	traMap::iterator checkTmp = global_tool_tra.find(id);
	if(checkDead(id, id)) 
	{
		// pthread_mutex_lock(&ydb_mutex);
		abort_release(tmpTra);
		tra.erase(id);
		pthread_cond_signal(&ydb_cond);
		pthread_mutex_unlock(&ydb_mutex);
		return ydb_protocol::ABORT;
	}
	while(locks[has])
		pthread_cond_wait(&ydb_cond, &ydb_mutex);
	cout << "也许被唤醒了" << endl;
	locks[has] = 1;
	sta.waiting = 0;
	tmpTra[has] = sta;
	tra[id] = tmpTra;
	cout << "del"<< has << "改变了map" << endl;
	prin();
	// cout << "del删除了" << has << endl;
	pthread_mutex_unlock(&ydb_mutex);
	return ydb_protocol::OK;
}

