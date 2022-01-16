// chfs client.  implements FS operations using extent and lock server
#include "chfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

chfs_client::chfs_client()
{
    ec = new extent_client();
}

chfs_client::chfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client();
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
}

chfs_client::inum
chfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
chfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
chfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_FILE) {
        printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    printf("isfile: %lld is not a file\n", inum);
    return false;
}

bool
chfs_client::isdir(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_DIR) {
        printf("isfile: %lld is a dir\n", inum);
        return true;
    } 
    printf("isfile: %lld is not a dir\n", inum);
    return false;
}

bool
chfs_client::issymlink(inum inum)
{
    extent_protocol::attr a;

    if (ec->getattr(inum, a) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }

    if (a.type == extent_protocol::T_SYMBOLLINK) {
        printf("isfile: %lld is a slink\n", inum);
        return true;
    } 
    printf("isfile: %lld is not a slink\n", inum);
    return false;
}


int 
chfs_client::readlink(inum inum, std::string &buf_out)
{
    int r=OK;
    ec->get(inum,buf_out);
    return r;
}

int
chfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    return r;
}

int
chfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
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
chfs_client::setattr(inum ino, size_t size)
{
    int r = OK;
    std::string buf;
    ec->get(ino,buf);
    buf.resize(size,'\0');
    ec->put(ino,buf);
    return r;
}

int
chfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;
    bool found;
    std::string buf;
    dirent_t item;

    if(lookup(parent,name,found,ino_out)==OK&&found){
        printf("creat dup name\n");
        return IOERR;
    }
    if(ec->create(extent_protocol::T_FILE,ino_out)!=OK)
        return IOERR;
    
    printf("line 181\n");
    ec->get(parent,buf);
    item.inum=ino_out;
    item.length=strlen(name);
    memcpy(item.name,name,item.length);
    printf("line 186\n");

    std::string temp((char*)(&item),sizeof(dirent_t));
    // buf.append((char*)(&item),sizeof(dirent_t));
    buf+=temp;
    printf("line 191\n");
    ec->put(parent,buf);
    printf("line 193\n");

    return r;
}

int
chfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;
    bool found=false;
    lookup(parent,name,found,ino_out);
    if(found)
        return EXIST;
    ec->create(extent_protocol::T_DIR,ino_out);
    std::string buf;
    ec->get(parent,buf);
    dirent_t item;
    item.inum=ino_out;
    item.length=strlen(name);
    memcpy(item.name,name,item.length);

    buf.append((char*)(&item),sizeof(dirent_t));
    ec->put(parent,buf);
    return r;
}

int
chfs_client::mksymlink(inum parent, const char *name, const char *linkTo, inum &ino_out)
{
    int r = OK;
    bool found=false;
    lookup(parent,name,found,ino_out);
    if(found)
        return EXIST;
    ec->create(extent_protocol::T_SYMBOLLINK,ino_out);
    ec->put(ino_out,std::string(linkTo));

    std::string buf;
    ec->get(parent,buf);
    dirent_t item;
    item.inum=ino_out;
    item.length=strlen(name);
    memcpy(item.name,name,item.length);
    buf.append((char*)(&item),sizeof(dirent_t));
    ec->put(parent,buf);

    return r;
}


int
chfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;
    std::list<dirent> list;

    readdir(parent,list);
    
    for(dirent item:list)
        if(item.name==std::string(name)){
            found=true;
            ino_out=item.inum;
            return r;
        }
    found=false;
    return r;
}

int
chfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;
    std::string buf;
    if(ec->get(dir,buf)!=OK)
        return IOERR;
    int itemNum=buf.size();
    int direntSize=sizeof(dirent_t);
    const char* ptr=buf.c_str();
    for(uint32_t i=0;i<(uint32_t)itemNum/direntSize;i++){
        dirent tempDirent;
        dirent_t tempDirent_t;
        memcpy((&tempDirent_t),ptr+i*direntSize,direntSize);
        tempDirent.inum=tempDirent_t.inum;
        tempDirent.name.assign(tempDirent_t.name,tempDirent_t.length);
        list.push_back(tempDirent);
    }
    return r;
}

int
chfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;
    std::string buf;
    if(ec->get(ino,buf)!=OK)
        return IOERR;
    if(off>=(long)buf.size())
        return r;
    data=buf.substr(off,size);
    return r;
}

int
chfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;
    std::string buf;
    ec->get(ino,buf);
    std::string writeStr;
    writeStr.assign(data,size);
    if(off>=(long)buf.size())
        buf.resize(off+size,'\0');
    bytes_written=size;
    buf.replace(off,size,writeStr);
    ec->put(ino,buf);
    return r;
}

int chfs_client::unlink(inum parent,const char *name)
{
    int r = OK;
    bool found=false;
    inum ino_out;
    lookup(parent,name,found,ino_out);
    ec->remove(ino_out);

    std::string buf, buf_in;
    if(ec->get(parent,buf)!=OK)
        return IOERR;
    int itemNum=buf.size();
    int direntSize=sizeof(dirent_t);
    const char* ptr=buf.c_str();
    for(uint32_t i=0;i<(uint32_t)itemNum/direntSize;i++){
        dirent_t tempDirent_t;
        memcpy((&tempDirent_t),ptr+i*direntSize,direntSize);
        if(tempDirent_t.inum!=ino_out)
            buf_in.append((char*)(&tempDirent_t),sizeof(dirent_t));
    }
    ec->put(parent,buf_in);
    return r;
}

