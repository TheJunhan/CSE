#include "ydb_server_2pl.h"
#include "extent_client.h"
#include <iostream>

using namespace std;

//#define DEBUG 1
//判断成环
bool check_circle(vector<vecotr<opt> >::iterator origin, vector<vecotr<opt> >::iterator current)
{
	
}
//判断死锁
bool ydb_server_2pl::judge(vector<vecotr<opt> >::iterator dst)
{
	
	return false;
}

bool check_own(vector<vector<opt> >::iterator dst, unsigned long long num, int caller)
{
	for(int i = 1; i < dst->size(); ++i)
	{
		if (i == caller) break;
		if (dst->at(i).vaid == num) return true;
	}
	return false;
}

//hash
unsigned long long xjh_hash(const std::string key)
{
	unsigned long long res = 0;
	for(int i = 0; i < key.size(); ++i)
	{
		res += (unsigned long long)key.at(i);
	}
	return (res % 1024);
}

//找到自己对应的transaction
vector<vector<opt> >::iterator ydb_server_2pl::find(ydb_protocol::transaction_id id)
{
	vector<vector<opt> >::iterator i = tra.begin();
	for (; i != tra.end(); i++)
	{
		if (i->at(0).id == id) 
			break;
	}
	return i;
}

ydb_server_2pl::ydb_server_2pl(std::string extent_dst, std::string lock_dst) : ydb_server(extent_dst, lock_dst) {
	current_id = 0;
}

ydb_server_2pl::~ydb_server_2pl() {
}

ydb_protocol::status ydb_server_2pl::transaction_begin(int, ydb_protocol::transaction_id &out_id) {    // the first arg is not used, it is just a hack to the rpc lib
	// lab3: your code here
	out_id = current_id;
	opt o(current_id, 0);
	o.vaid = -1;
	vector<opt> tmp;
	tmp.push_back(o);
	tra.push_back(tmp);
	current_id++;
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_2pl::transaction_commit(ydb_protocol::transaction_id id, int &) {
	// lab3: your code here
	//输出看看
	for (int i = 0; i < tra.size(); ++i)
	{
		for (int j = 0; j < tra.at(i).size(); ++j)
		{
			cout << tra.at(i).at(j).op << " " << tra.at(i).at(j).content << " ";
		}
		cout << endl;
	}
	vector<vector<opt> >::iterator dst = find(id);
	// for(vector<opt>::iterator i = dst.begin(); i != dst.end(); ++i)
	// {
	// 	//把值写入es，其他操作情况不需要在这里处理
	// 	//begin = 0, get = 1, set = 2, del = 3
	// 	if(i->op == 2) 
	// 	{
	// 		ec->put(i->vaid, i->content);
	// 	}
	// 	else if(i->op == 3)
	// 	{
	// 		ec->remove(i->vaid);
	// 	}
	// 	else if(i->op == 0) continue;
	// 	lc->release(i->vaid);
	// }
	// cout << dst->size() << endl;
	for(int i = 0; i < dst->size(); ++i)
	{
		// cout << "这里是" << dst->at(i).vaid << endl;
		if(dst->at(i).op == 2)
		{
			ec->put(dst->at(i).vaid, dst->at(i).content);
		}
		else if(dst->at(i).op == 3)
		{
			ec->remove(dst->at(i).vaid);
		}
		else if(dst->at(i).op == 0) continue;
		if(!check_own(dst, dst->at(i).vaid, i)) 
		{
			cout << "将要release" << dst->at(i).vaid << endl;	
			lc->release(dst->at(i).vaid);
		}
	}
	// cout << "I'm out!" << endl;
	tra.erase(dst);
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_2pl::transaction_abort(ydb_protocol::transaction_id id, int &) {
	// lab3: your code here
	for (vector<vector<opt> >::iterator i = tra.begin(); i != tra.end(); ++i)
	{
		if(i->at(0).id == id)
		{
			tra.erase(i);
			break;
		}
	}
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_2pl::get(ydb_protocol::transaction_id id, const std::string key, std::string &out_value) {
	// lab3: your code here
	//首先判断之前是否读过
	unsigned long long has = xjh_hash(key);
	vector<vector<opt> >::iterator dst = find(id);
	for(int i = 0; i < dst->size(); ++i) 
	{
		if (dst->at(i).vaid == has)
		{
			out_value = dst->at(i).content;
			return ydb_protocol::OK;
		}
	}
	//没读过
	ec->get(has, out_value);
	opt o(id, 1);
	o.vaid = has;
	o.content = out_value;
	dst->push_back(o);
	if(judge(dst))
	{
		return ydb_protocol::ABORT;
	}
	if(!check_own(dst, has, dst->size() - 1)) 
	{
		cout << "get将要acquire" << has << endl;
		lc->acquire(has);
	}
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_2pl::set(ydb_protocol::transaction_id id, const std::string key, const std::string value, int &) {
	// lab3: your code here
	unsigned long long has = xjh_hash(key);
	vector<vector<opt> >::iterator dst = find(id);
	opt o(id, 2);
	o.vaid = has;
	o.content = value;
	dst->push_back(o);
	if(judge(dst))
	{
		return ydb_protocol::ABORT;
	}
	if(!check_own(dst, has, dst->size() - 1)) 
	{
		cout << "set将要acquire" << has << endl;
		lc->acquire(has);
	}
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_2pl::del(ydb_protocol::transaction_id id, const std::string key, int &) {
	// lab3: your code here
	vector<vector<opt> >::iterator dst = find(id);
	unsigned long long has = xjh_hash(key);
	opt o(id, 3);
	o.vaid = has;
	dst->push_back(o);
	if(judge(dst))
	{
		return ydb_protocol::ABORT;
	}
	if(!check_own(dst, has, dst->size() - 1)) 
	{
		cout << "del将要acquire" << has << endl;
		lc->acquire(has);
	}
	return ydb_protocol::OK;
}

