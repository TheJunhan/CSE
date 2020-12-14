// the extent server implementation

#include "extent_cache_server.h"
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <iostream>

using namespace std;

extent_cache_server::extent_cache_server() 
{
  im = new inode_manager();
}

extent_cache_server::~extent_cache_server()
{}

int extent_cache_server::create(string cid, uint32_t type, extent_protocol::extentid_t &id)
{
  // alloc a new inode and return inum
  id = im->alloc_inode(type);

  return extent_protocol::OK;
}

int extent_cache_server::put(string cid, extent_protocol::extentid_t id, std::string buf, int &)
{
  id &= 0x7fffffff;
  const char * cbuf = buf.c_str();
  int size = buf.size();
  im->write_file(id, cbuf, size);
  //首先记住这个client也会记录此cache
  Holding::iterator dst = holdingData.find(id);
  if(dst != holdingData.end())
  {
    dst->second.insert(cid);
  }
  else 
  {
    set<string> tmp;
    tmp.insert(cid);
    holdingData.insert({id, tmp});
  }
  //提醒其他client
  reminder(cid, id);
  return extent_protocol::OK;
}

int extent_cache_server::get(string cid, extent_protocol::extentid_t id, std::string &buf)
{
  // printf("extent_cache_server: get %lld\n", id);
  id &= 0x7fffffff;

  int size = 0;
  char *cbuf = NULL;

  im->read_file(id, &cbuf, &size);
  if (size == 0)
    buf = "";
  else {
    buf.assign(cbuf, size);
    free(cbuf);
  }
  
  //给cache增加元素
  Holding::iterator dst = holdingData.find(id);
  if(dst != holdingData.end())
  {
    dst->second.insert(cid);
  }
  else 
  {
    set<string> tmp;
    tmp.insert(cid);
    holdingData.insert({id, tmp});
  }

  return extent_protocol::OK;
}

int extent_cache_server::getattr(string cid, extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  id &= 0x7fffffff;
  
  extent_protocol::attr attr;
  memset(&attr, 0, sizeof(attr));
  im->getattr(id, attr);
  a = attr;

  //给Attrcache增加元素
  Holding::iterator dst = holdingAttr.find(id);
  if(dst != holdingAttr.end())
  {
    dst->second.insert(cid);
  }
  else 
  {
    set<string> tmp;
    tmp.insert(cid);
    holdingAttr.insert({id, tmp});
  }

  return extent_protocol::OK;
}

int extent_cache_server::remove(string cid, extent_protocol::extentid_t id, int &)
{
  // printf("extent_cache_server: write %lld\n", id);
  id &= 0x7fffffff;
  im->remove_file(id);
  //提醒其他client
  reminder(cid, id);
  //删去关于此的记录
  Holding::iterator dst = holdingData.find(id);
  if(dst != holdingData.end()) holdingData.erase(dst);
  dst = holdingAttr.find(id);
  if(dst != holdingAttr.end()) holdingAttr.erase(dst);
  return extent_protocol::OK;
}

void extent_cache_server::reminder(string cid, extent_protocol::extentid_t eid)
{
  int r;
  //关于数据cache
  Holding::iterator dst = holdingData.find(eid);
  if(dst != holdingData.end()) 
  {
    clients tmp = dst->second;

    for(clients::iterator i = tmp.begin(); i != tmp.end(); ++i)
    {
      if(*i == cid) continue; 
      //处理数据
      handle(*i).safebind()->call(extent_protocol::remind, eid, r);
      tmp.erase(i);
    }
    holdingData.insert({eid, tmp});
  }
  //关于attrcache
  dst = holdingAttr.find(eid);
  if(dst != holdingAttr.end()) 
  {
    clients attrclients = dst->second;

    for(clients::iterator i = attrclients.begin(); i != attrclients.end(); ++i)
    {
      handle(*i).safebind()->call(extent_protocol::remind, eid, r);
    }

    holdingAttr.erase(dst);
  }
}