#ifndef extent_cache_client_h
#define extent_cache_client_h

#include <map>
#include <string>
#include "extent_cache_server.h"
#include "extent_protocol.h"

using namespace std;

typedef map<extent_protocol::extentid_t, string> DataCache;

class extent_cache_client
{
private:
    DataCache cache;
    rpcc *cl;

public:
    extent_cache_client(std::string dst);
    ~extent_cache_client();

    // int remind(extent_cache_protocol::extentid_t dstid);

    extent_protocol::status create(uint32_t type, extent_protocol::extentid_t &eid);
    extent_protocol::status get(extent_protocol::extentid_t eid, 
                                        std::string &buf);
    extent_protocol::status getattr(extent_protocol::extentid_t eid, 
                                            extent_protocol::attr &a);
    extent_protocol::status put(extent_protocol::extentid_t eid, std::string buf);
    extent_protocol::status remove(extent_protocol::extentid_t eid);
};

#endif