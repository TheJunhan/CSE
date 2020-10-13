#include "inode_manager.h"
#include <ctime>
#include <stdio.h>
#include <iostream>
using namespace std;
//工具函数
//检查blockid和目标字符串合法性
bool checkBlockId(blockid_t dst, const char *buf)
{
  if(dst < 0 || dst >= BLOCK_NUM || !buf) 
  {
    //std::cout << "illegal blockid or buf" << std::endl;
    printf("illegal blockid!\n");
    return false;
  }
  return true;
}

// disk layer -----------------------------------------
//this line is from xjh to test whether it's possible to be change the docker from outside
disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
  if(checkBlockId(id - 1, buf))
  memcpy(buf, blocks[id - 1], BLOCK_SIZE);
  //printf("read %b block: %s", id, *buf);
}

void
disk::write_block(blockid_t id, const char *buf)
{
  if(checkBlockId(id - 1, buf))
  memcpy(blocks[id - 1], buf, BLOCK_SIZE);
  //printf("write %b block: %s", id, *buf);
}

// block layer -----------------------------------------
//找到合适的block
// blockid_t find_fit_block(int begin, int end, blockid_t start) 
// {
//   return 1;
// }

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
    /*
     * your code goes here.
     * note: you should mark the corresponding bit in block bitmap when alloc.
     * you need to think about which block you can start to be allocated.
     */
    /*
    * uint32_t bid = 0;
    while (bid + IBLOCK(INODE_NUM, BLOCK_NUM) <= BLOCK_NUM)
    {
        char rec[BLOCK_SIZE];
        d->read_block(BBLOCK(bid), rec);
        char mask = 0x80;
        //当前bid在bitmap中是第几个
        uint32_t tmp = bid - (BBLOCK(bid) - 2) * BLOCK_NUM * 8;
        //如果对应的bitmap是空的
        if (!(rec[tmp / 8] & (mask >> bid % 8)))
        {
            rec[tmp / 8] = rec[tmp / 8] | (mask >> (bid % 8));
            d->write_block(BBLOCK(bid), rec);
            return bid + IBLOCK(INODE_NUM, BLOCK_NUM);
        }
        bid++;
    }
    */
    for (int i = IBLOCK(INODE_NUM, BLOCK_NUM); i <= BLOCK_NUM; ++i) if (!using_blocks[i]) {
        using_blocks[i] = 1;
        return i;
    }
    printf("\t\tNo free block, sorry about that.\n");
    exit(-1);
}

void
block_manager::free_block(uint32_t id)
{
    /*
     * your code goes here.
     * note: you should unmark the corresponding bit in the block bitmap when free.
     */
    /*
    * uint32_t bid = id - IBLOCK(INODE_NUM, BLOCK_NUM);
    uint32_t tmp = bid - (BBLOCK(bid) - 2) * BLOCK_SIZE * 8;
    char buf[BLOCK_SIZE];
    d->read_block(BBLOCK(bid), buf);
    char mask = 0x80;
    buf[tmp / 8] = buf[tmp / 8] & ~(mask >> (bid % 8));
    d->write_block(BBLOCK(bid), buf);
    */
    using_blocks[id] = 0;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();
  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;
}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
  //找到第一个空闲block
  uint32_t inum = 1;
  inode* tmp;
  while((tmp = get_inode(inum))) {
    free(tmp);
    inum++;
  }
  //create
  tmp = (inode*)malloc(sizeof(inode));
  tmp->type = (uint32_t)type;
  tmp->size = 0;
  tmp->atime = (unsigned int)time(0);
  tmp->mtime = (unsigned int)time(0);
  tmp->ctime = (unsigned int)time(0);

  put_inode(inum, tmp);
  return inum;
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
  inode* dst = get_inode(inum);
  if(dst->type == 0) return;
  int blockNum = (dst->size == 0) ? 0 : ((dst->size - 1) / BLOCK_SIZE + 1);
  //free blocks
  //without NIND
  if(blockNum <= NDIRECT)
  {
    for(int i = 0; i < blockNum; ++i) bm->free_block(dst->blocks[i]);
  }
  //with
  else
  {
    for(int i = 0; i < NDIRECT; ++i) bm->free_block(dst->blocks[i]);
    blockid_t list[NINDIRECT];
    bm->read_block(dst->blocks[NDIRECT], (char *)list);
    for(int i = NDIRECT; i < blockNum; ++i)
    {
      bm->free_block(list[i - NDIRECT]);
    }
  }
  //free inode
  dst->type = 0;
  put_inode(inum, dst);
  free(dst);
 }


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  printf("\tim: get_inode %d\n", inum);

  if (inum < 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode*)buf + inum%IPB;
  if (ino_disk->type == 0) {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a,b) ((a)<(b) ? (a) : (b))

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_Out
   */
  inode* dst = get_inode(inum);
  int final_size = dst->size;
  if (final_size == 0) return;
  char* res = (char *)malloc(BLOCK_NUM * BLOCK_SIZE);
  int blockNum = (final_size - 1) / BLOCK_SIZE + 1;
  if(blockNum <= NDIRECT) 
  {
    for (int i = 0; i < blockNum; ++i) bm->read_block(dst->blocks[i], res + BLOCK_SIZE * i);
  }
  else
  {
    for(int i = 0; i < NDIRECT; ++i) bm->read_block(dst->blocks[i], res + BLOCK_SIZE * i);
    blockid_t list[NINDIRECT];
    bm->read_block(dst->blocks[NDIRECT], (char*)list);
    for(int i = NDIRECT; i < blockNum; ++i) bm->read_block(list[i - NDIRECT], res + BLOCK_SIZE * i);
  }
  // int *re_size = (int *)malloc(sizeof(int));
  // *re_size = final_size;
  dst->atime = time(0);
  put_inode(inum, dst);
  free(dst);
  *buf_out = res;
  *size = final_size;
  // cout << "call read" << endl;
  // for(int i = 0; i < final_size; ++i) cout << res[i];
  // cout <<endl;
  // cout << *size << endl;
  // cout << "end call" << endl;
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */
  if(size > MAXFILE * BLOCK_SIZE) {
    printf("\t\tFile too large!\n");
    return;
  }
  inode *dst = get_inode(inum);
  uint32_t blockNumBefore = (dst->size - 1)/BLOCK_SIZE + 1;
  uint32_t blockNumAfter = (size - 1)/BLOCK_SIZE + 1;
  if(dst->size == 0) blockNumBefore = 0;
  if(size == 0) blockNumAfter = 0;
  //后面比前面多
  if(size > dst->size)
  {
    //都没用到IND
    if(blockNumAfter <= NDIRECT) 
    {
      for(int i = blockNumBefore; i < blockNumAfter; ++i)
      {
        dst->blocks[i] = bm->alloc_block();
      }
      for(int i = 0; i < blockNumAfter; ++i) 
      {
        bm->write_block(dst->blocks[i], buf + BLOCK_SIZE * i);
        char tmp_buf[BLOCK_SIZE];
        bm->read_block(dst->blocks[i], tmp_buf);
      }
    }
    //后面的要用到前面的不用
    else if(blockNumAfter > NDIRECT && blockNumBefore <= NDIRECT)
    {
      for(int i = 0; i < blockNumBefore; ++i) bm->write_block(dst->blocks[i], buf + BLOCK_SIZE * i);
      for(int i = blockNumBefore; i < NDIRECT; ++i) 
      {
        dst->blocks[i] = bm->alloc_block();
        bm->write_block(dst->blocks[i], buf + BLOCK_SIZE * i);
      }
      //设置nind
      dst->blocks[NDIRECT] = bm->alloc_block();
      blockid_t list[NINDIRECT];
      for(int i = NDIRECT; i < blockNumAfter; ++i)
      {
        list[i - NDIRECT] = bm->alloc_block();
        bm->write_block(list[i - NDIRECT], buf + BLOCK_SIZE * i);
      }
      bm->write_block(dst->blocks[NDIRECT], (char *)list);
    }
    //都用到了
    else if(blockNumBefore > NDIRECT)
    {
      for(int i = 0; i < NDIRECT; ++i) 
      {
        bm->write_block(dst->blocks[i], buf + BLOCK_SIZE * i);
      }
      blockid_t list[NINDIRECT];
      bm->read_block(dst->blocks[NDIRECT], (char*)list);
      for(int i = NDIRECT; i < blockNumBefore; ++i)
      {
        bm->write_block(list[i - NDIRECT], buf + BLOCK_SIZE * i);
      }
      for(int i = blockNumBefore; i < blockNumAfter; ++i) 
      {
        list[i - NDIRECT] = bm->alloc_block();
        bm->write_block(list[i - NDIRECT], buf + BLOCK_SIZE * i);
      }
    }
  }
  //相等
  else if(size == dst->size)
  {
    if(blockNumAfter <= NDIRECT)
    {
      for (int i = 0; i < blockNumAfter; ++i) 
      {
        bm->write_block(dst->blocks[i], buf + BLOCK_SIZE * i);
      }
    }
    if(blockNumAfter > NDIRECT)
    {
      for(int i = 0; i < NDIRECT; ++i)
      {
        bm->write_block(dst->blocks[i], buf + BLOCK_SIZE * i);
      }
      blockid_t list[NINDIRECT];
      bm->read_block(dst->blocks[NDIRECT], (char*)list);
      for(int i = NDIRECT; i < blockNumAfter; ++i)
      {
        bm->write_block(list[i - NDIRECT], buf + BLOCK_SIZE * i);
      }
    }
  }
  //后面比前面少
  else
  {
    //都少于IND
    if(blockNumBefore <= NDIRECT) 
    {
      for(int i = 0; i < blockNumAfter; ++i) 
      {
        bm->write_block(dst->blocks[i], buf + BLOCK_SIZE * i);
      }
      for(int i = blockNumAfter; i < blockNumBefore; ++i)
      {
        bm->free_block(dst->blocks[i]);
      }
    }
    //一个大一个小
    if(blockNumBefore > NDIRECT && blockNumAfter <= NDIRECT)
    {
      //写
      for(int i = 0; i < blockNumAfter; ++i)
      {
        bm->write_block(dst->blocks[i], buf + BLOCK_SIZE * i);
      }
      //释放
      for(int i = blockNumAfter; i < NDIRECT; ++i)
      {
        bm->free_block(dst->blocks[i]);
      }
      blockid_t list[NINDIRECT];
      bm->read_block(dst->blocks[NDIRECT], (char*)list);
      for(int i = NDIRECT; i < blockNumBefore; ++i)
      {
        bm->free_block(list[i - NDIRECT]);
      }
      bm->free_block(dst->blocks[NDIRECT]);
    }
    //两个都用到了ind
    else if(blockNumAfter > NDIRECT)
    {
      for(int i = 0; i < NDIRECT; ++i) 
      {
        bm->write_block(dst->blocks[i], buf + BLOCK_SIZE * i);
      }
      blockid_t list[NINDIRECT];
      bm->read_block(dst->blocks[NDIRECT], (char*)list);
      for (int i = NDIRECT; i < blockNumAfter; ++i) 
      {
        bm->write_block(list[i - NDIRECT], buf + BLOCK_SIZE * i);
      }
      for(int i = blockNumAfter; i < blockNumBefore; ++i) bm->free_block(list[i - NDIRECT]);
    }
  }
  dst->size = size;
  dst->atime = time(0);
  dst->ctime = time(0);
  dst->mtime = time(0);
  put_inode(inum, dst);
  free(dst);
  
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  //printf("here we go");
  inode *dst = get_inode(inum);
  if(!dst) {
    printf("wrong get_inode in getattr");
    return;
    }
  a.atime = dst->atime;
  a.mtime = dst->mtime;
  a.ctime = dst->ctime;
  a.type = (uint32_t)dst->type;
  a.size = dst->size;
  free(dst);
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
  free_inode(inum);
}
