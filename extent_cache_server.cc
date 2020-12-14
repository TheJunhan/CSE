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
  // cout << "using cache server" << endl;
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
  // cout << "读了：id = " << id << "cbuf = " << cbuf;
  return extent_protocol::OK;
}

int extent_cache_server::getattr(string cid, extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  // printf("extent_cache_server: getattr %lld\n", id);

  id &= 0x7fffffff;
  
  extent_protocol::attr attr;
  memset(&attr, 0, sizeof(attr));
  im->getattr(id, attr);
  a = attr;

  return extent_protocol::OK;
}

int extent_cache_server::remove(string cid, extent_protocol::extentid_t id, int &)
{
  // printf("extent_cache_server: write %lld\n", id);
  
  id &= 0x7fffffff;
  im->remove_file(id);
 
  return extent_protocol::OK;
}

