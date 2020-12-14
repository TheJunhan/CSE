// RPC stubs for clients to talk to extent_server
#define USE_CACHE

#include "extent_cache_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>
#include <string>
#include <random>

using namespace std;

extent_cache_client::extent_cache_client(std::string dst)
{
  sockaddr_in dstsock;
  srand(time(0));
  myIdInt = rand();
  myIdInt += rand();
  myId = to_string(myIdInt);
  // myId += to_string(rand());
  make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0) {
    printf("extent_client: bind failed\n");
  }
  rpcs *rerpc = new rpcs(myIdInt, 20);
  rerpc->reg(extent_protocol::remind, this, &extent_cache_client::remind_handler);
}

// a demo to show how to use RPC
extent_protocol::status
extent_cache_client::create(uint32_t type, extent_protocol::extentid_t &id)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here
  ret = cl->call(extent_protocol::create, myId, type, id);
  VERIFY(ret == extent_protocol::OK);
  return ret;
}

extent_protocol::status
extent_cache_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here
  //检查是否有cache
#ifdef USE_CACHE
  DataCache::iterator dst = datacache.find(eid);
  //有
  if(dst != datacache.end())
  {
    buf = dst->second;
    return ret;
  }
#endif
  //无
  ret = cl->call(extent_protocol::get, myId, eid, buf);
#ifdef USE_CACHE
  datacache.insert({eid, buf});
#endif
  VERIFY(ret == extent_protocol::OK);
  return ret;
}

extent_protocol::status
extent_cache_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
  extent_protocol::status ret = extent_protocol::OK;
  //首先检查是否有cache
#ifdef USE_CACHE
  AttrCache::iterator dst = attrcache.find(eid);
  if(dst != attrcache.end())
  {
    attr = dst->second;
    return ret;
  }
  //无
#endif
  ret = cl->call(extent_protocol::getattr, myId, eid, attr);
#ifdef USE_CACHE
  attrcache.insert({eid, attr});
#endif
  return ret;
}

extent_protocol::status
extent_cache_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  int r;
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here
#ifdef USE_CACHE
  //首先检查自己是否有cache
  DataCache::iterator dst = datacache.find(eid);
  //有
  if(dst != datacache.end())
  {
    dst->second = buf;
  }
  //无
  else
  {
    datacache.insert({eid, buf});
  }
#endif
  //接下来正常操作即可
  ret = cl->call(extent_protocol::put, myId, eid, buf, r);
  VERIFY(ret == extent_protocol::OK);
  return ret;
}

extent_protocol::status
extent_cache_client::remove(extent_protocol::extentid_t eid)
{
  int r;
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here
#ifdef USE_CACHE
  //首先检查自己是否有cache
  DataCache::iterator dst = datacache.find(eid);
  //有
  if(dst != datacache.end())
  {
    datacache.erase(dst);
  }
#endif
  //接下来正常操作即可
  ret = cl->call(extent_protocol::remove, myId, eid, r);
  VERIFY(ret == extent_protocol::OK);
  return ret;
}

extent_protocol::status extent_cache_client::remind_handler(extent_protocol::extentid_t eid, int &)
{
  DataCache::iterator dst = datacache.find(eid);
  if(dst != datacache.end())
  {
    // if(content == "") datacache.erase(dst);
    // else dst->second = content;
    datacache.erase(dst);
  }

  AttrCache::iterator attrdst = attrcache.find(eid);
  if(attrdst != attrcache.end())
  {
    attrcache.erase(attrdst);
  }

  return extent_protocol::OK;
}