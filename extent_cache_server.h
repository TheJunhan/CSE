//cache_for_data
#ifndef extent_cache_server_h
#define extent_cache_server_h

#include "extent_protocol.h"
#include "inode_manager.h"
#include "handle.h"
#include <map>
#include <string>
#include <set>

using namespace std;

typedef set<string> clients;
typedef map<extent_protocol::extentid_t, clients> Holding;

class extent_cache_server
{
private:
    inode_manager *im;
    Holding holdingData;
    Holding holdingAttr;
public:
    extent_cache_server();
    ~extent_cache_server();

    int create(string cid, uint32_t type, extent_protocol::extentid_t &id);
    int put(string cid, extent_protocol::extentid_t id, std::string, int &);
    int get(string cid, extent_protocol::extentid_t id, std::string &);
    int getattr(string cid, extent_protocol::extentid_t id, extent_protocol::attr &);
    int remove(string cid, extent_protocol::extentid_t id, int &);
    //提醒客户端把数据删掉
    void reminder(string cid, extent_protocol::extentid_t eid);
};

#endif