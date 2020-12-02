// // RPC stubs for clients to talk to lock_server, and cache the locks
// // see lock_client.cache.h for protocol details.

// #include "lock_client_cache.h"
// #include "rpc.h"
// #include <sstream>
// #include <iostream>
// #include <stdio.h>
// #include <sys/time.h>
// #include "tprintf.h"


// int lock_client_cache::last_port = 0;

// lock_client_cache::lock_client_cache(std::string xdst,
//                                      class lock_release_user *_lu)
//         : lock_client(xdst), lu(_lu) {
//     srand(time(NULL) ^ last_port);
//     rlock_port = ((rand() % 32000) | (0x1 << 10));
//     const char *hname;
//     // VERIFY(gethostname(hname, 100) == 0);
//     hname = "127.0.0.1";
//     std::ostringstream host;
//     host << hname << ":" << rlock_port;
//     id = host.str();
//     last_port = rlock_port;
//     rpcs *rlsrpc = new rpcs(rlock_port);
//     rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
//     rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);
//     pthread_mutex_init(&lockManagerLock, NULL);
// }

// lock_protocol::status
// lock_client_cache::acquire(lock_protocol::lockid_t lid) {

//     pthread_mutex_lock(&lockManagerLock);
//     if (lockManager.find(lid) == lockManager.end()) lockManager[lid] = new LockEntry();
//     LockEntry *lockEntry = lockManager[lid];
//     QueuingThread *thisThread = new QueuingThread();
//     if (lockEntry->threads.empty()) {
//         LockState s = lockEntry->state;
//         lockEntry->threads.push_back(thisThread);
//         if (s == FREE) {
//             lockEntry->state = LOCKED;
//             pthread_mutex_unlock(&lockManagerLock);
//             return lock_protocol::OK;
//         } else if (s == NONE) {
//             return blockUntilGot(lockEntry, lid, thisThread);
//         } else {
//             pthread_cond_wait(&thisThread->cv, &lockManagerLock);
//             return blockUntilGot(lockEntry, lid, thisThread);
//         }
//     } else {
//         lockEntry->threads.push_back(thisThread);
//         pthread_cond_wait(&thisThread->cv, &lockManagerLock);
//         switch (lockEntry->state) {
//             case FREE:
// 		lockEntry->state = LOCKED;
//             case LOCKED:
//                 pthread_mutex_unlock(&lockManagerLock);
//                 return lock_protocol::OK;
//             case NONE:
//                 return blockUntilGot(lockEntry, lid, thisThread);
//             default:
//                 assert(0);
//         }
//     }
// }


// lock_protocol::status
// lock_client_cache::release(lock_protocol::lockid_t lid) {

//     pthread_mutex_lock(&lockManagerLock);
//     int r;
//     int ret = rlock_protocol::OK;
//     LockEntry *lockEntry = lockManager[lid];
//     bool fromRevoked = false;
//     if (lockEntry->message == REVOKE) {
//         fromRevoked = true;
//         lockEntry->state = RELEASING;

//         pthread_mutex_unlock(&lockManagerLock);
//         ret = cl->call(lock_protocol::release, lid, id, r);
//         pthread_mutex_lock(&lockManagerLock);
//         lockEntry->message = EMPTY;
//         lockEntry->state = NONE;
//     } else lockEntry->state = FREE;

//     delete lockEntry->threads.front();
//     lockEntry->threads.pop_front();
//     if (lockEntry->threads.size() >= 1) {
//         if (!fromRevoked) lockEntry->state = LOCKED;
//         pthread_cond_signal(&lockEntry->threads.front()->cv);
//     }
//     pthread_mutex_unlock(&lockManagerLock);
//     return ret;

// }

// rlock_protocol::status
// lock_client_cache::revoke_handler(lock_protocol::lockid_t lid,
//                                   int &) {
//     int r;
//     int ret = rlock_protocol::OK;
//     pthread_mutex_lock(&lockManagerLock);
//     LockEntry *lockEntry = lockManager[lid];
//     if (lockEntry->state == FREE) {
//         lockEntry->state = RELEASING;
//         pthread_mutex_unlock(&lockManagerLock);
//         ret = cl->call(lock_protocol::release, lid, id, r);
//         pthread_mutex_lock(&lockManagerLock);
//         lockEntry->state = NONE;
//         if (lockEntry->threads.size() >= 1) {
//             pthread_cond_signal(&lockEntry->threads.front()->cv);
//         }
//     } else { lockEntry->message = REVOKE; }
//     pthread_mutex_unlock(&lockManagerLock);
//     return ret;
// }

// rlock_protocol::status
// lock_client_cache::retry_handler(lock_protocol::lockid_t lid,
//                                  int &) {
//     int ret = rlock_protocol::OK;
//     pthread_mutex_lock(&lockManagerLock);
//     LockEntry *lockEntry = lockManager[lid];
//     lockEntry->message = RETRY;
//     pthread_cond_signal(&lockEntry->threads.front()->cv);
//     pthread_mutex_unlock(&lockManagerLock);
//     return ret;
// }


// lock_protocol::status lock_client_cache::
// blockUntilGot(lock_client_cache::LockEntry *lockEntry,
//               lock_protocol::lockid_t lid,
//               QueuingThread *thisThread) {
//     int r;
//     lockEntry->state = ACQUIRING;
//     while (lockEntry->state == ACQUIRING) {
//         pthread_mutex_unlock(&lockManagerLock);
//         int ret = cl->call(lock_protocol::acquire, lid, id, r);
//         pthread_mutex_lock(&lockManagerLock);
//         if (ret == lock_protocol::OK) {
//             lockEntry->state = LOCKED;
//             pthread_mutex_unlock(&lockManagerLock);
//             return lock_protocol::OK;
//         } else {
//             if (lockEntry->message == EMPTY) {
//                 pthread_cond_wait(&thisThread->cv, &lockManagerLock);
//                 lockEntry->message = EMPTY;
//             } else lockEntry->message = EMPTY;
//         }
//     }
//     assert(0);
// }

// lock_client_cache::~lock_client_cache() {
//     pthread_mutex_lock(&lockManagerLock);
//     std::map<lock_protocol::lockid_t, LockEntry *>::iterator iter;
//     for (iter = lockManager.begin(); iter != lockManager.end(); iter++)
//         delete iter->second;
//     pthread_mutex_unlock(&lockManagerLock);
// }


// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"

using namespace std;

int lock_client_cache::last_port = 0;

lock_client_cache::lock_client_cache(std::string xdst, 
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu)
{
  cout << "启动的是我" << endl;
  srand(time(NULL)^last_port);
  rlock_port = ((rand()%32000) | (0x1 << 10));
  const char *hname;
  // VERIFY(gethostname(hname, 100) == 0);
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlock_port;
  id = host.str();
  last_port = rlock_port;
  rpcs *rlsrpc = new rpcs(rlock_port);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);
}

/* acquire contains sleep
 */
lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
  cout << "你lc调用acquire了" << lid << endl;
  pthread_mutex_lock(&mtx);
  if (lock_keeper.find(lid) == lock_keeper.end()) {
    // this type of lock hasn't appeared
    lock_manager.insert(std::pair<lock_protocol::lockid_t, pthread_cond_t>(lid, PTHREAD_COND_INITIALIZER));
    lock_keeper.insert(std::pair<lock_protocol::lockid_t, client_status>(lid, NONE));
    revoke_has_arrived.insert(std::pair<lock_protocol::lockid_t, bool>(lid, false));
  }
  // tprintf("acquire in client %s for %d status: %d\n", id.c_str(), lid, lock_keeper[lid]);

  if (lock_keeper[lid] == NONE) {
    // the client has no idea about the lock, need to acquire from server
    REACQUIRE:
    lock_keeper[lid] = ACQUIRING;
    int r;
    pthread_mutex_unlock(&mtx);  

    lock_protocol::status ret = cl->call(lock_protocol::acquire, lid, id, r);

    pthread_mutex_lock(&mtx);  
    // ret may be OK or RETRY
    if (ret == lock_protocol::RETRY) {
      // tprintf("acquire in client %s for %d NONE -> ACQUIRING\n", id.c_str(), lid);
      while (lock_keeper[lid] != FREE && lock_keeper[lid] != LOCKED) {
        if (lock_keeper[lid] == NONE)
          goto REACQUIRE;
        pthread_cond_wait(&(lock_manager.find(lid)->second), &mtx);
      }
      // tprintf("acquire in client %s for %d FREE -> LOCKED\n", id.c_str(), lid);
      lock_keeper[lid] = LOCKED;
      
      pthread_mutex_unlock(&mtx);
      return lock_protocol::OK;
    }
    // tprintf("acquire in client %s for %d NONE -> LOCKED\n", id.c_str(), lid);
    lock_keeper[lid] = LOCKED;
    
    pthread_mutex_unlock(&mtx);  
    return lock_protocol::OK;
  }
  
  // the client is holding the lock
  while (lock_keeper[lid] != FREE) {
    if (lock_keeper[lid] == NONE)
      goto REACQUIRE;
    pthread_cond_wait(&(lock_manager.find(lid)->second), &mtx);
  }
  // tprintf("acquire in client %s for %d -> LOCKED\n", id.c_str(), lid);
  lock_keeper[lid] = LOCKED;

  pthread_mutex_unlock(&mtx);  
  return lock_protocol::OK;
}

/* release doesn't contain any sleep
 */
lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
  cout << "你lc调用release了" << lid << endl;
  pthread_mutex_lock(&mtx);
  // tprintf("release in client %s for %d status: %d\n", id.c_str(), lid, lock_keeper[lid]);
  if (lock_keeper[lid] != LOCKED) {
    pthread_mutex_unlock(&mtx);
    // tprintf("STATUSERR!!\n");
    return lock_protocol::STATUSERR;
  }

  if (revoke_has_arrived[lid]) {
    // need to grant lock back to server right now
    // tprintf("release in client %s for %d LOCKED -> RELEASING\n", id.c_str(), lid);
    lock_keeper[lid] = RELEASING;
    int r;
    pthread_mutex_unlock(&mtx);

    int ret = cl->call(lock_protocol::release, lid, id, r);
    
    pthread_mutex_lock(&mtx);
    // ret should be OK
    // tprintf("release in client %s for %d RELEASING -> NONE\n", id.c_str(), lid);
    lock_keeper[lid] = NONE;
    revoke_has_arrived[lid] = false;
    pthread_cond_signal(&(lock_manager.find(lid)->second));

    pthread_mutex_unlock(&mtx);
    return lock_protocol::OK;
  }

  // tprintf("release in client %s for %d LOCKED -> FREE\n", id.c_str(), lid);
  lock_keeper[lid] = FREE;
  pthread_cond_signal(&(lock_manager.find(lid)->second));

  pthread_mutex_unlock(&mtx);
  return lock_protocol::OK;
}

/* revoke handler doesn't contain any sleep
 */
rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid, int &)
{
  pthread_mutex_lock(&mtx);
  // tprintf("revoke in client %s for %d status: %d\n", id.c_str(), lid, lock_keeper[lid]);
  if (lock_keeper[lid] == FREE) {
    // tprintf("revoke in client %s for %d FREE -> RELEASING\n", id.c_str(), lid);
    lock_keeper[lid] = RELEASING;
    int r;
    pthread_mutex_unlock(&mtx);

    int ret = cl->call(lock_protocol::release, lid, id, r);
    
    pthread_mutex_lock(&mtx);
    if (ret == lock_protocol::OK) {
      // tprintf("revoke in client %s for %d RELEASING -> NONE\n", id.c_str(), lid);
      lock_keeper[lid] = NONE;
    }
    pthread_cond_signal(&(lock_manager.find(lid)->second));

    pthread_mutex_unlock(&mtx);
    return rlock_protocol::OK;
  }

  revoke_has_arrived[lid] = true;

  pthread_mutex_unlock(&mtx);
  return rlock_protocol::RETRY;
}

/* retry handler doesn't contain any sleep
 */
rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid, int &)
{
  pthread_mutex_lock(&mtx);
  // tprintf("retry in client %s for %d status: %d\n", id.c_str(), lid, lock_keeper[lid]);
  if (lock_keeper[lid] != NONE && lock_keeper[lid] != ACQUIRING) {
    pthread_mutex_unlock(&mtx);
    // tprintf("STATUSERR\n");
    return rlock_protocol::STATUSERR;
  }

  if (lock_keeper[lid] == NONE) {
    pthread_mutex_unlock(&mtx);
    return rlock_protocol::OK;
  }

  if (lock_keeper[lid] == ACQUIRING) {
    // tprintf("retry in client %s for %d ACQUIRING -> FREE\n", id.c_str(), lid);
    lock_keeper[lid] = LOCKED;
    pthread_cond_signal(&(lock_manager.find(lid)->second));
    
    pthread_mutex_unlock(&mtx);
    return rlock_protocol::OK;
  }

  assert(0);
}
