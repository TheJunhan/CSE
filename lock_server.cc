// // the lock server implementation

// #include "lock_server.h"
// #include <sstream>
// #include <stdio.h>
// #include <unistd.h>
// #include <arpa/inet.h>

// lock_server::lock_server():
//   nacquire (0)
// {
//   pthread_mutex_init(&mutex, NULL);
// }

// lock_protocol::status
// lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
// {
//   lock_protocol::status ret = lock_protocol::OK;
//   printf("stat request from clt %d\n", clt);
//   r = nacquire;
//   return ret;
// }

// lock_protocol::status
// lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
// {
//   lock_protocol::status ret = lock_protocol::OK;
// 	// Your lab2 part2 code goes here
//   pthread_mutex_lock(&mutex);
//   //不存在
//   if(locks.find(lid) == locks.end())
//   {
//     pthread_cond_t cond;
//     pthread_cond_init(&cond, NULL);
//     conds[lid] = cond;
//   }
//   //存在
//   else if (locks[lid])
//   {
//     while (locks[lid]) pthread_cond_wait(&conds[lid], &mutex);
//   }
//   locks[lid] = true;
//   pthread_mutex_unlock(&mutex);
//   r = ret;
//   return ret;
// }

// lock_protocol::status
// lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
// {
//   lock_protocol::status ret = lock_protocol::OK;
// 	// Your lab2 part2 code goes here
//   pthread_mutex_lock(&mutex);
//   locks[lid] = false;
//   pthread_cond_signal(&conds[lid]);
//   pthread_mutex_unlock(&mutex);
//   r = ret;
//   return ret;
// }

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

pthread_mutex_t lock_server::mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t lock_server::cond = PTHREAD_COND_INITIALIZER;

lock_server::lock_server(): nacquire (0)
{
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab2 part2 code goes here
  pthread_mutex_lock(&mutex);
  if (lock_status_table.find(lid) == lock_status_table.end()) 
    lock_status_table[lid] =  LOCKED;
  else {
    //自旋
    while(lock_status_table[lid] == LOCKED)
      pthread_cond_wait(&cond, &mutex);
    lock_status_table[lid] = LOCKED;
  }
  pthread_mutex_unlock(&mutex);
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab2 part2 code goes here
  pthread_mutex_lock(&mutex);
  if (lock_status_table.find(lid) == lock_status_table.end())
    ret = lock_protocol::NOENT;
  else {
    lock_status_table[lid] = UNLOCKED;
    pthread_cond_signal(&cond);
  }
  pthread_mutex_unlock(&mutex);
  return ret;
}