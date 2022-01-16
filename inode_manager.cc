#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk(){
  bzero(blocks, sizeof(blocks));
}

void disk::read_block(blockid_t id, char *buf){

  if(id<0||id>=BLOCK_NUM) return;
  memcpy(buf,blocks[id],BLOCK_SIZE);
}

void disk::write_block(blockid_t id, const char *buf){
  if(id<0||id>=BLOCK_NUM) return;
  memcpy(blocks[id],buf,BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t block_manager::alloc_block(){
  for(blockid_t i=IBLOCK(INODE_NUM,sb.nblocks)+1;i<BLOCK_NUM;i++)
    if(!using_blocks[i]){
      using_blocks[i]=1;
      return i;
    }
    exit(0);
}

void block_manager::free_block(uint32_t id){
  using_blocks[id]=0;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager(){
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;
}

void block_manager::read_block(uint32_t id, char *buf){
  d->read_block(id, buf);
}

void block_manager::write_block(uint32_t id, const char *buf){
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager(){
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t inode_manager::alloc_inode(uint32_t type){
  for(unsigned int inum=1;inum<=INODE_NUM;inum++)
    if(!get_inode(inum)){
      inode* inodeToUse=new inode();
      inodeToUse->size=0;
      inodeToUse->type=type;
      inodeToUse->atime=time(0);
      inodeToUse->ctime=time(0);
      inodeToUse->mtime=time(0);
      put_inode(inum,inodeToUse);
      free(inodeToUse);
      return inum;
    }
  exit(0);
}

void inode_manager::free_inode(uint32_t inum){
    inode* inode_=get_inode(inum);
    if(!inode_)return;
    inode_->size=0;
    inode_->type=0;
    put_inode(inum,inode_);
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode*  inode_manager::get_inode(uint32_t inum){
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  printf("\tim: get_inode %d\n", inum);

  if (inum < 0 || inum >= INODE_NUM) {
    printf("\tim: inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);

  ino_disk = (struct inode*)buf + inum%IPB;
  if (ino_disk->type == 0) {
    printf("\tim: inode not exist\n");
    return NULL;
  }

  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void inode_manager::put_inode(uint32_t inum, struct inode *ino){
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
void inode_manager::read_file(uint32_t inum, char **buf_out, int *size){
  inode* inode_=get_inode(inum);
  if(!inode_)return;

  *size=inode_->size;
  int blockNum=(*size-1)/BLOCK_SIZE+1;
  char* buf=(char*)malloc(BLOCK_SIZE*blockNum);
  for(int i=0;i<blockNum;i++){
    int blockth=findNthBolckNum(inode_,i);
    bm->read_block(blockth,buf+BLOCK_SIZE*i);
  }
  *buf_out=buf;
  inode_->atime=time(0);
  put_inode(inum,inode_);
}

/* alloc/free blocks if needed */
void inode_manager::write_file(uint32_t inum, const char *buf, int size){
  printf("size: %d\n",size);
  inode* inode_=get_inode(inum);
  if(!inode_) return;
  int blockNum=(size-1)/BLOCK_SIZE+1;
  int previousBlockNum=(inode_->size>0)?(inode_->size-1)/BLOCK_SIZE+1:0;

  char temp_buf[BLOCK_SIZE*blockNum];
  memcpy(temp_buf,buf,size);

  if(blockNum<previousBlockNum)
    for(int i=previousBlockNum-1;i>=blockNum;i--)
      freeNthBolck(inode_,i);
  else if(blockNum>previousBlockNum)
    for(int i=previousBlockNum;i<blockNum;i++)
      allocNthBolck(inode_,i);
  printf("inode_manager.cc line 167 blockNum=%d\n",blockNum);

  for(int i=0;i<blockNum;i++){
    blockid_t block_number=findNthBolckNum(inode_,i);
    printf("i=%d block_number=%d\n",i,block_number);
    bm->write_block(block_number,temp_buf+BLOCK_SIZE*i);
    printf("write block_number=%d finish\n",block_number);
  }
  printf("line 172\n");

  inode_->size=size;
  inode_->mtime=time(0);
  inode_->ctime=time(0);
  inode_->atime=time(0);
  put_inode(inum,inode_);
  printf("line 179\n");

}

void inode_manager::getattr(uint32_t inum, extent_protocol::attr &a){
  inode* inode_=get_inode(inum);
  if(!inode_)return;
  a.atime=inode_->atime;
  a.ctime=inode_->ctime;
  a.mtime=inode_->mtime;
  a.size=inode_->size;
  a.type=inode_->type;
  free(inode_);
}

void inode_manager::remove_file(uint32_t inum){
  inode* inode_=get_inode(inum);
  if(!inode_)return;
  int blockNum=(inode_->size>0)?(inode_->size-1)/BLOCK_SIZE+1:0;
  for(int i=0;i<blockNum;i++){
    int blockth=findNthBolckNum(inode_,i);
    bm->free_block(blockth);
  }
  free_inode(inum);  
}


blockid_t inode_manager::findNthBolckNum(inode* inode_, uint32_t nth){
  if(nth<NDIRECT)
    return inode_->blocks[nth];
  char buf[BLOCK_SIZE];
  bm->read_block(inode_->blocks[NDIRECT],buf);
  return ((blockid_t*)buf)[nth-NDIRECT];
}

void inode_manager::allocNthBolck(inode* inode_, uint32_t nth){
  if(nth<NDIRECT){
    inode_->blocks[nth] = bm->alloc_block();
    return;
  }
  if(nth==NDIRECT){
    int indirectBlockNum=bm->alloc_block();
    inode_->blocks[NDIRECT]=indirectBlockNum;
  }
  
  int blockNum=bm->alloc_block();
  char buf[BLOCK_SIZE];
  bm->read_block(inode_->blocks[NDIRECT],buf);
  ((blockid_t*)buf)[nth-NDIRECT]=blockNum;
  bm->write_block(inode_->blocks[NDIRECT],buf);
}

void inode_manager::freeNthBolck(inode* inode_, uint32_t nth){
  blockid_t blockNum=findNthBolckNum(inode_,nth);
  bm->free_block(blockNum);
  if(nth==NDIRECT)
    bm->free_block(inode_->blocks[NDIRECT]);
}
