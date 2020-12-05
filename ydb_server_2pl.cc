#include "ydb_server_2pl.h"
#include "extent_client.h"
#include <iostream>

using namespace std;

pthread_mutex_t ydb_server_2pl::ydb_mutex = PTHREAD_MUTEX_INITIALIZER;
//#define DEBUG 1
void ydb_server_2pl::prin()
{
	for (int i = 0; i < tra.size(); ++i)
	{
		for (int j = 1; j < tra.at(i).size(); ++j)
		{
			cout << tra.at(i).at(j).id << " " << tra.at(i).at(j).vaid << " " << tra.at(i).at(j).waiting << " " << tra.at(i).at(j).content << " ";
		}
		cout << endl;
	}
}
//找到依赖
vector<vector<opt> >::iterator ydb_server_2pl::depend(vector<vector<opt> >::iterator originorigin, vector<vector<opt> >::iterator origin)
{
	unsigned long long dst_vaid = origin->at(origin->size() - 1).vaid;
	vector<vector<opt> >::iterator i = tra.begin();
	for(; i != tra.end(); ++i)
	{
		if(i == origin) continue;
		for(int j = 1; j < i->size(); ++j)
		{
			//对i存在依赖
			if(dst_vaid == i->at(j).vaid && !i->at(j).waiting)
			{
				if(i == originorigin && j == originorigin->size() - 1) continue;
				cout << origin->at(0).id << "依赖" << i->at(j).id << endl;
				prin();
				return i;
			}
		}
	}
	//不存在依赖关系
	return tra.end();
}
//判断成环
bool ydb_server_2pl::check_circle(vector<vector<opt> >::iterator origin, vector<vector<opt> >::iterator last, vector<vector<opt> >::iterator current)
{
	if(origin == current) return true;
	vector<vector<opt> >::iterator next = depend(last, current);
	if(next == tra.end()) return false;
	return check_circle(origin, current, next);
}
//判断死锁
bool ydb_server_2pl::judge(vector<vector<opt> >::iterator dst)
{
	//找到第一个依赖
	vector<vector<opt> >::iterator first = depend(dst, dst);
	if(first == tra.end()) return false;
	return check_circle(dst, dst, first);
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
	pthread_mutex_lock(&ydb_mutex);
	out_id = current_id;
	// cout << out_id << "调用了begin" << endl;
	opt o(current_id, 0);
	o.vaid = -1;
	vector<opt> tmp;
	tmp.push_back(o);
	tra.push_back(tmp);
	current_id++;
	pthread_mutex_unlock(&ydb_mutex);
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_2pl::transaction_commit(ydb_protocol::transaction_id id, int &) {
	// lab3: your code here
	//输出看看
	// for (int i = 0; i < tra.size(); ++i)
	// {
	// 	for (int j = 0; j < tra.at(i).size(); ++j)
	// 	{
	// 		cout << tra.at(i).at(j).op << " " << tra.at(i).at(j).id << " " << tra.at(i).at(j).content << " ";
	// 	}
	// 	cout << endl;
	// }
	pthread_mutex_lock(&ydb_mutex);
	vector<vector<opt> >::iterator dst = find(id);
	if(dst == tra.end()) 
	{
		pthread_mutex_unlock(&ydb_mutex);
		return ydb_protocol::TRANSIDINV;
	}
	// cout << id << "调用了commit" << endl;
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
			cout << "transaction" << dst->at(i).id << "将要release" << dst->at(i).vaid << endl;
			lc->release(dst->at(i).vaid);
		}
	}
	// cout << "I'm out!" << endl;
	tra.erase(dst);
	pthread_mutex_unlock(&ydb_mutex);
	return ydb_protocol::OK;
}

void ydb_server_2pl::abort_release(vector<vector<opt> >::iterator i)
{
	for(int j = 0; j < i->size(); ++j)
	{
		if(i->at(j).op == 0) continue;
		cout << "由于abort了，将要release" << i->at(j).vaid << endl;
		lc->release(i->at(j).vaid);
	}
}

ydb_protocol::status ydb_server_2pl::transaction_abort(ydb_protocol::transaction_id id, int &) {
	// lab3: your code here
	pthread_mutex_lock(&ydb_mutex);
	for (vector<vector<opt> >::iterator i = tra.begin(); i != tra.end(); ++i)
	{
		if(i->at(0).id == id)
		{
			//放锁
			// for(int j = 0; j < i->size(); ++j)
			// {
			// 	if(i->at(j).op == 0) continue;
			// 	cout << "由于abort了，将要release" << i->at(j).vaid << endl;
			// 	lc->release(i->at(j).vaid);
			// }
			abort_release(i);
			tra.erase(i);
			break;
		}
	}
	pthread_mutex_unlock(&ydb_mutex);
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_2pl::get(ydb_protocol::transaction_id id, const std::string key, std::string &out_value) {
	// lab3: your code here
	//首先判断之前是否读过或者写过
	pthread_mutex_lock(&ydb_mutex);

	unsigned long long has = xjh_hash(key);
	vector<vector<opt> >::iterator dst = find(id);
	if(dst == tra.end()) 
	{
		pthread_mutex_unlock(&ydb_mutex);
		return ydb_protocol::TRANSIDINV;
	}

	bool has_touch = false;
	for(int i = 0; i < dst->size(); ++i) 
	{
		if (dst->at(i).vaid == has)
		{
			has_touch = true;
			out_value = dst->at(i).content;
		}
	}
	if(has_touch) 
	{
		pthread_mutex_unlock(&ydb_mutex);
		return ydb_protocol::OK;
	}
	//没读过
	ec->get(has, out_value);
	opt o(id, 1);
	o.vaid = has;
	o.waiting = true;
	o.content = out_value;
	dst->push_back(o);
	if(judge(dst))
	{
		abort_release(dst);
		tra.erase(dst);
		pthread_mutex_unlock(&ydb_mutex);
		return ydb_protocol::ABORT;
	}
	if(!check_own(dst, has, dst->size() - 1)) 
	{
		cout << "transction" << dst->at(0).id << "get将要acquire" << has << endl;
		pthread_mutex_unlock(&ydb_mutex);
		lc->acquire(has);
		pthread_mutex_lock(&ydb_mutex);
		dst->at(dst->size() - 1).waiting = false;
	}
	pthread_mutex_unlock(&ydb_mutex);
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_2pl::set(ydb_protocol::transaction_id id, const std::string key, const std::string value, int &) {
	// lab3: your code here
	pthread_mutex_lock(&ydb_mutex);
	unsigned long long has = xjh_hash(key);
	// cout << "把" << key << "哈希到了" << has << endl;
	vector<vector<opt> >::iterator dst = find(id);
	if(dst == tra.end()) return ydb_protocol::TRANSIDINV;

	opt o(id, 2);
	o.vaid = has;
	o.content = value;
	o.waiting = true;
	dst->push_back(o);
	if(judge(dst))
	{
		//处理
		abort_release(dst);
		tra.erase(dst);
		pthread_mutex_unlock(&ydb_mutex);
		return ydb_protocol::ABORT;
	}
	if(!check_own(dst, has, dst->size() - 1)) 
	{
		cout << "transction" << dst->at(0).id << "set将要acquire" << has << endl;
		pthread_mutex_unlock(&ydb_mutex);
		lc->acquire(has);
		pthread_mutex_lock(&ydb_mutex);
		dst->at(dst->size() - 1).waiting = false;
		cout << "我改变了transaction：" << dst->at(0).id << "第" << dst->size() - 1 << "个的waiting为：" << dst->at(dst->size() - 1).waiting << endl;
	}
	pthread_mutex_unlock(&ydb_mutex);
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_2pl::del(ydb_protocol::transaction_id id, const std::string key, int &) {
	// lab3: your code here
	pthread_mutex_lock(&ydb_mutex);
	unsigned long long has = xjh_hash(key);
	vector<vector<opt> >::iterator dst = find(id);
	if(dst == tra.end()) 
	{
		pthread_mutex_unlock(&ydb_mutex);
		return ydb_protocol::TRANSIDINV;
	}

	opt o(id, 3);
	o.vaid = has;
	o.waiting = true;
	dst->push_back(o);
	if(judge(dst))
	{
		abort_release(dst);
		tra.erase(dst);
		pthread_mutex_unlock(&ydb_mutex);
		return ydb_protocol::ABORT;
	}
	if(!check_own(dst, has, dst->size() - 1)) 
	{
		cout << "transction" << dst->at(0).id << "del将要acquire" << has << endl;
		pthread_mutex_unlock(&ydb_mutex);
		lc->acquire(has);
		pthread_mutex_lock(&ydb_mutex);
		dst->at(dst->size() - 1).waiting = false;
	}
	pthread_mutex_unlock(&ydb_mutex);
	return ydb_protocol::OK;
}

