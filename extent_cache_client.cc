// RPC stubs for clients to talk to extent_server

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
  myId = to_string(rand());
  // myId += to_string(rand());
  cout << "this is myID" << myId << endl;
  make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0) {
    printf("extent_client: bind failed\n");
  }
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
  ret = cl->call(extent_protocol::get, myId, eid, buf);
  VERIFY(ret == extent_protocol::OK);
  return ret;
}

extent_protocol::status
extent_cache_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
  extent_protocol::status ret = extent_protocol::OK;
  ret = cl->call(extent_protocol::getattr, myId, eid, attr);
  return ret;
}

extent_protocol::status
extent_cache_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  int r;
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here
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
  ret = cl->call(extent_protocol::remove, myId, eid, r);
  VERIFY(ret == extent_protocol::OK);
  return ret;
}