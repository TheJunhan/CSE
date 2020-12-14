// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string>
#include <ctime>
using namespace std;

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
    // ec = new extent_client(extent_dst);
    ec = new extent_cache_client(extent_dst);
    // Lab2: Use lock_client_cache when you test lock_cache
    // lc = new lock_client(lock_dst);
    lc = new lock_client_cache(lock_dst);
    lc->acquire(1);
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
    lc->release(1);
}

yfs_client::inum
yfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
yfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
yfs_client::isfile(inum inum)
{
    extent_protocol::attr a;
    lc->acquire(inum);
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        // printf("error getting attr\n");
        lc->release(inum);
        return false;
    }
    lc->release(inum);

    if (a.type == extent_protocol::T_FILE) {
        // printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    // printf("isfile: %lld is a dir\n", inum);
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

bool
yfs_client::isdir(inum inum)
{
    // Oops! is this still correct when you implement symlink?
    extent_protocol::attr a;
    lc->acquire(inum);
    if(ec->getattr(inum, a) != extent_protocol::OK) 
    {
        // printf("wrong in isdir in yfs_client.cc!");
        lc->release(inum);
        return false;
    }
    lc->release(inum);
    if(a.type == extent_protocol::T_DIR) return true;
    return false;
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    // printf("getfile %016llx\n", inum);
    // printf("%d拿到了锁：", inum);
    lc->acquire(inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        lc->release(inum);
        goto release;
    }
    lc->release(inum);
    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    // printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    // printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    lc->acquire(inum);
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        lc->release(inum);
        goto release;
    }
    lc->release(inum);
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int
yfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    string buf;
    lc->acquire(ino);
    ec->get(ino, buf);
    buf.resize(size);
    ec->put(ino, buf);
    lc->release(ino);
    return r;
}

int yfs_client::addFile(inum parent, const char *name, mode_t mode, inum &ino_out, extent_protocol::types type)
{
    bool found = false;
    lc->acquire(parent);
    lookup(parent, name, found, ino_out);
    if(found) 
    {
        lc->release(parent);
        return EXIST;
    }
    std::string str_name = (std::string)name;
    for(size_t i = 0; i < str_name.size(); ++i) if(str_name[i] == ',' || str_name[i] == ';') 
    {
        return IOERR;
    }
    std::string content;
    ec->create(type, ino_out);
    ec->get(parent, content);
    content += str_name + ',' + filename(ino_out) + ';';
    ec->put(parent, content);
    lc->release(parent);
    return OK;
}

int
yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{

    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    return addFile(parent, name, mode, ino_out, extent_protocol::T_FILE);
}

int
yfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    return addFile(parent, name, mode, ino_out, extent_protocol::T_DIR);
}

int
yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    std::string content;
    ec->get(parent, content);
    std::string dst = (std::string)name;
    for(size_t i = 0; i < content.size(); ++i)
    {
        std::string filename;
        while(content[i] != ',') 
        {
            filename += content[i];
            i++;
        }
        i++;
        if(filename == dst) {
            found = true;
            std::string num;
            while(content[i] != ';')
            {
                num += content[i];
                i++;
            }
            ino_out = n2i(num);
            return r;
        }
        while(content[i] != ';') i++;
    }
    found = false;
    return r;
}

int
yfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    std::string content;
    lc->acquire(dir);
    ec->get(dir, content);
    for(size_t i = 0; i < content.size(); ++i) 
    {
        std::string filename;
        std::string num;
        while(content[i] != ',') 
        {
            filename += content[i];
            i++;
        }
        i++;
        while(content[i] != ';')
        {
            num += content[i];
            i++;
        }
        inum tmp = n2i(num);
        dirent info;
        info.inum = tmp;
        info.name = filename;
        list.push_back(info);
    }
    lc->release(dir);
    return r;
}

int
yfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;
    /*
     * your code goes here.
     * note: read using ec->get().
     */
    if(off < 0) {
        return RPCERR;
    }
    string buf;
    lc->acquire(ino);
    ec->get(ino, buf);
    if (off <= buf.size())
    {
        if (off + size <= buf.size())
            data = buf.substr(off, size);
        else
            data = buf.substr(off, buf.size() - off);
    }
    //超出范围了、
    else 
      data = "";
    lc->release(ino);
    return r;
}

int
yfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;

    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
    string dst = string(data, size);
    string buf;
    lc->acquire(ino);
    ec->get(ino, buf);
    if(off + size > buf.size()) buf.resize(off + size, '\0');
    buf.replace(off, size, dst);
    bytes_written = size;
    ec->put(ino, buf);
    lc->release(ino);
    return r;
}

int yfs_client::unlink(inum parent,const char *name)
{
    int r = OK;
    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
    string dstname = (string)name;
    string buf, res;
    inum dstino;
    bool flag = false;
    lc->acquire(parent);
    ec->get(parent, buf);
    for(size_t i = 0; i < buf.size(); ++i)
    {
        string tmpstr;
        string tmpin;
        while(buf[i] != ',') 
        {
            tmpstr += buf[i];
            i++;
        }
        i++;
        //发现了
        if(tmpstr == dstname) 
        {
            flag = true;
            while(buf[i] != ';') 
            {
                tmpin += buf[i];
                i++;
                dstino = n2i(tmpin);
            }
            continue;
        }
        //并不是
        while(buf[i] != ';')
        {
            tmpin += buf[i];
            i++;
        }
        res += tmpstr + ',' + tmpin + ';';
    }
    if(!flag) 
    {
        return IOERR;
    }
    ec->put(parent, res);
    //释放要删除的文件占用的blocks
    //yfs_remove(dstino);
    lc->acquire(dstino);
    ec->remove(dstino);
    lc->release(dstino);
    lc->release(parent);
    return r;
}


void yfs_client::yfs_remove(yfs_client::inum id)
{
    extent_protocol::attr att;
    ec->getattr(id, att);
    if(att.type == extent_protocol::T_DIR) 
    {
        list<dirent> lis;
        readdir(id, lis);
        while(lis.size())
        {
            yfs_remove(lis.front().inum);
            lis.pop_front();
        }
    }
    ec->remove(id);
}

int yfs_client::symlink(const char* link, inum parent, const char* name, inum &ino)
{
    mode_t mode = 0;
    int r = extent_protocol::OK;
    
    if(addFile(parent, name, mode, ino, extent_protocol::T_SYMLK) != extent_protocol::OK) 
    {
        return IOERR;
    }
    
    lc->acquire(ino);
    ec->put(ino, (string)link);
    lc->release(ino);
    return r;
}

int yfs_client::getsymlink(inum ino, symlinkinfo &info)
{
    int r = extent_protocol::OK;
    extent_protocol::attr a;

    lc->acquire(ino);
    if(ec->getattr(ino, a) != extent_protocol::OK)
    {
        lc->release(ino);
        return IOERR;
    }
    lc->release(ino);

    info.atime = a.atime;
    info.ctime = a.ctime;
    info.mtime = a.mtime;
    info.size = a.size;
    return r;
}

int yfs_client::readlink(inum ino, string &link)
{
    int r = extent_protocol::OK;
    lc->acquire(ino);
    if(ec->get(ino, link) != extent_protocol::OK) 
    {
        lc->release(ino);
        return IOERR;
    }
    lc->release(ino);
    return r;
}

