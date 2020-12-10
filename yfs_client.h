#ifndef yfs_client_h
#define yfs_client_h

#include <string>
#include "lock_protocol.h"
#include "lock_client.h"
#include "lock_client_cache.h"
//#include "yfs_protocol.h"
#include "extent_client.h"
#include <vector>


class yfs_client {
  extent_client *ec;
  lock_client *lc;
  // lock_client_cache *lc;
 public:

  typedef unsigned long long inum;
  enum xxstatus { OK, RPCERR, NOENT, IOERR, EXIST };
  typedef int status;

  struct fileinfo {
    unsigned long long size;
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirinfo {
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirent {
    std::string name;
    yfs_client::inum inum;
  };
    struct symlinkinfo
  {
    unsigned long long size;
    unsigned int atime;
    unsigned int mtime;
    unsigned int ctime;
  };

 private:
  static std::string filename(inum);
  static inum n2i(std::string);
  void yfs_remove(inum id);

 public:
  yfs_client(std::string, std::string);
  int addFile(inum parent, const char *name, mode_t mode, inum &ino_out, extent_protocol::types type);

  bool isfile(inum);
  bool isdir(inum);

  int getfile(inum, fileinfo &);
  int getdir(inum, dirinfo &);

  int setattr(inum, size_t);
  int lookup(inum, const char *, bool &, inum &);
  int create(inum, const char *, mode_t, inum &);
  int readdir(inum, std::list<dirent> &);
  int write(inum, size_t, off_t, const char *, size_t &);
  int read(inum, size_t, off_t, std::string &);
  int unlink(inum,const char *);
  int mkdir(inum , const char *, mode_t , inum &);
  
  /** you may need to add symbolic link related methods here.*/
  int symlink(const char* link, inum parent, const char* name, inum &ino);
  int readlink(inum ino, std::string &link);
  int getsymlink(inum, symlinkinfo &);
};

#endif 
