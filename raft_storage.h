#ifndef raft_storage_h
#define raft_storage_h

#include <fcntl.h>

#include <fstream>
#include <mutex>

#include "raft_protocol.h"
#include "raft_state_machine.h"
#include "raft_test_utils.h"

template <typename command>
std::ofstream &operator<<(std::ofstream &m, const log_entry<command> &entry) {
  // Your code here
  m << entry.command_ << "\t" << entry.termNumber << "\t" << entry.index;
  return m;
}

template <typename command>
std::ifstream &operator>>(std::ifstream &u, log_entry<command> &entry) {
  // Your code here
  u >> entry.command_ >> entry.termNumber >> entry.index;
  return u;
}

template <typename command>
class raft_storage {
#define ASSERT(condition, message)                                            \
  do {                                                                        \
    if (!(condition)) {                                                       \
      std::cerr << "Assertion `" #condition "` failed in " << __FILE__ << ":" \
                << __LINE__ << " msg: " << message << std::endl;              \
      std::terminate();                                                       \
    }                                                                         \
  } while (false)

 public:
  raft_storage(const std::string &file_dir);
  ~raft_storage();
  // Your code here
  void pushBackEntrys(std::vector<log_entry<command>> &newEntrys);
  void rewriteAndAddEntrys(std::vector<log_entry<command>> &newEntrys);
  void pushBackEntry(log_entry<command> &newEntry);
  void pop_back();
  void setNextIndexs(int index, int number);
  void decreaceNextIndexs(int index);
  void initializeNextIndexs(int size, int initial);
  void setMatchedIndexs(int index, int number);
  void initializeMatchedIndexs(int size, int initial);
  int  readCurrent_term();
  int  LogVectorsize();
  void saveToDir() ;
  void saveMetaDataToDir(int current_term) ;
  void saveNextIndexsToDir() ;
  void saveMatchedIndexsToDir() ;
  void saveLogToDir() ;
  void saveSnapShotToDir() ;
  log_entry<command> back();
  int currentLatestIndex();
  std::vector<log_entry<command>> entriesAfterIndex(int index);
  command &getCommandByIndex(int index);
  log_entry<command> getEntryByIndex(int index);
  std::vector<int> getNextIndexs() ;
  int getNextIndexs(int index) ;
  std::vector<int> getMatchedIndexs() ;
  int getMatchedIndexs(int index) ;
  void emptyLogs(int lastIncludedIndex, int lastIncludedTerm);
  void setSnapshotData(std::vector<char> snapshotData);
  std::vector<char> getSnapshotData();

  int snapshot_index;//已经被包含在snapshot中的后一个（即已包含在snapshot中）
  void eraseBeforeLastIncluded(int lastIncluded);
  int readSnapShotIndexFromDir();


 private:
  std::vector<log_entry<command>> entryVector;
  std::mutex log_mtx;
  std::mutex snapshot_mtx;
  std::string file_dir;
  std::vector<int> nextIndexs;
  std::vector<int> matchedIndexs;
  std::vector<char> snapshotData;

};

template <typename command>
raft_storage<command>::raft_storage(const std::string &dir) : file_dir(dir) {
  // Your code here
  std::lock_guard<std::mutex> guard(log_mtx);

  // std::cout << "read logs from " + file_dir + "/log.txt" << std::endl;
  std::ifstream in_log(file_dir + "/log.txt");
  if (in_log.is_open() && !in_log.eof()) {
    unsigned n;
    in_log >> n;
    // std::cout << n << " elements" << std::endl;
    entryVector = std::vector<log_entry<command>>(n);
    for (unsigned i = 0; i < n; i++) {
      // std::cout << i << " finish" << std::endl;
      log_entry<command> z;
      in_log >> z;
      // std::cout << z.index<<std::endl;
      entryVector[i] = z;
    }
  }
  else{
    log_entry<command> placeholder;
    placeholder.index = 0;
    placeholder.termNumber = 0;
    entryVector = std::vector<log_entry<command>>(1, placeholder);
  }
  in_log.close();

  std::lock_guard<std::mutex> guard2(snapshot_mtx);

  // std::cout << "read snapshot from " + file_dir + "/snapshot.txt" << std::endl;
  std::ifstream in_snap(file_dir + "/snapshot.txt");
  if (in_snap.is_open() && !in_snap.eof()) {
    std::string snapshot_index_str;
    std::getline(in_snap, snapshot_index_str);
    snapshot_index = std::stoi(snapshot_index_str);

    std::string snapshotData_str;
    std::getline(in_snap, snapshotData_str);
    snapshotData = std::vector<char>();
    snapshotData.assign(snapshotData_str.begin(), snapshotData_str.end());

  }
  else{
    snapshot_index = 0;
    snapshotData = std::vector<char>();
  }
  in_snap.close();
}


template <typename command>
int raft_storage<command>::readSnapShotIndexFromDir() {
  std::lock_guard<std::mutex> guard(snapshot_mtx);
  int ret = 0;
  std::ifstream in_snap(file_dir + "/snapshot.txt");
  if (in_snap.is_open() && !in_snap.eof()) {
    std::string snapshot_index_str;
    std::getline(in_snap, snapshot_index_str);
    ret = std::stoi(snapshot_index_str);
  }
  return ret;
}


template <typename command>
raft_storage<command>::~raft_storage() {
  // saveToDir();
}

template <typename command>
int raft_storage<command>::readCurrent_term() {
  int cu = 0;
  std::ifstream in_meta(file_dir + "/meta_data.txt");
  if (in_meta.is_open() && !in_meta.eof()) 
    in_meta >> cu;
  in_meta.close();
  return cu;
}

template <typename command>
void raft_storage<command>::pushBackEntrys(
    std::vector<log_entry<command>> &newEntrys) {
  std::lock_guard<std::mutex> guard(log_mtx);
  entryVector.insert(entryVector.end(), newEntrys.begin(), newEntrys.end());
  saveLogToDir();
}

template <typename command>
void raft_storage<command>::pushBackEntry(log_entry<command> &newEntry) {
  std::lock_guard<std::mutex> guard(log_mtx);
  entryVector.push_back(newEntry);
  saveLogToDir();
}

template <typename command>
void raft_storage<command>::pop_back() {
  std::lock_guard<std::mutex> guard(log_mtx);
  ASSERT(entryVector.size() != 1, "can not remove placeholder");
  entryVector.pop_back();
  saveLogToDir();
}

template <typename command>
log_entry<command> raft_storage<command>::back()  {
  ASSERT(entryVector.size() != 0, "vector empty");
  return entryVector.back();
}

/*delete all the entries from fromIndex, without*/
template <typename command>
int raft_storage<command>::currentLatestIndex()  {
  return entryVector.size() + snapshot_index - 1;
}//2 3-> 2+4-1 = 5 

template <typename command>
int raft_storage<command>::LogVectorsize()  {
  // Your code here
  return entryVector.size();
}

template <typename command>
std::vector<log_entry<command>> raft_storage<command>::entriesAfterIndex(
    int index)  {
  std::vector<log_entry<command>> after;

  for (const log_entry<command> &entry : entryVector)
    if (entry.index >= index) after.push_back(entry);
  return after;
}

template <typename command>
command &raft_storage<command>::getCommandByIndex(int index) {
  ASSERT(entryVector.size() > index -snapshot_index && index -snapshot_index >=0, "vector error");
  return entryVector[index -snapshot_index].command_;
}

template <typename command>
log_entry<command> raft_storage<command>::getEntryByIndex(int index)  {
  
  ASSERT(entryVector.size() > index -snapshot_index && index -snapshot_index >=0, "vector error");
  return entryVector[index -snapshot_index];
}

template <typename command>
void raft_storage<command>::eraseBeforeLastIncluded(int lastIncluded)  {
  std::lock_guard<std::mutex> guard(log_mtx);
  while(entryVector.front().index < lastIncluded)
    entryVector.erase(entryVector.begin());
  saveLogToDir();
}

template <typename command>
void raft_storage<command>::emptyLogs(int lastIncludedIndex, int lastIncludedTerm)  {
  std::lock_guard<std::mutex> guard(log_mtx);
  log_entry<command> placeholder;
  placeholder.index = lastIncludedIndex;
  placeholder.termNumber = lastIncludedTerm;
  entryVector = std::vector<log_entry<command>>(1, placeholder);
  saveLogToDir();
}

template <typename command>
std::vector<int> raft_storage<command>::getNextIndexs()  {
  return nextIndexs;
}

template <typename command>
void raft_storage<command>::setNextIndexs(int index, int number) {
  ASSERT(nextIndexs.size() > index, "vector empty");
  nextIndexs[index] = number;
  // saveNextIndexsToDir();
}

template <typename command>
int raft_storage<command>::getNextIndexs(int index)  {
  ASSERT(nextIndexs.size() > index, "vector empty");
  return nextIndexs[index];
}

template <typename command>
void raft_storage<command>::decreaceNextIndexs(int index) {
  ASSERT(nextIndexs.size() > index, "vector empty");
  nextIndexs[index]--;
  // saveNextIndexsToDir();
}

template <typename command>
void raft_storage<command>::initializeNextIndexs(int size, int initial) {
  nextIndexs = std::vector<int>(size, initial);
  // saveNextIndexsToDir();
}

template <typename command>
std::vector<int> raft_storage<command>::getMatchedIndexs()  {
  return matchedIndexs;
}

template <typename command>
void raft_storage<command>::setMatchedIndexs(int index, int number) {
  ASSERT(matchedIndexs.size() > index, "vector empty");
  matchedIndexs[index] = number;
  // saveMatchedIndexsToDir();
}

template <typename command>
void raft_storage<command>::initializeMatchedIndexs(int size, int initial) {
  matchedIndexs = std::vector<int>(size, initial);
  // saveMatchedIndexsToDir();
}

template <typename command>
int raft_storage<command>::getMatchedIndexs(int index)  {

  ASSERT(matchedIndexs.size() > index, "vector empty");
  return matchedIndexs[index];
}

template <typename command>
void raft_storage<command>::saveToDir()  {
  saveLogToDir();
}

template <typename command>
void raft_storage<command>::setSnapshotData(std::vector<char> snapshotData_){
  std::lock_guard<std::mutex> guard(snapshot_mtx);
  snapshotData = snapshotData_;
  // std::cout<<"set snapshotData size "<<snapshotData_.size();
  saveSnapShotToDir();
}

template <typename command>
std::vector<char> raft_storage<command>::getSnapshotData(){
  std::lock_guard<std::mutex> guard(snapshot_mtx);
  return snapshotData;
}



template <typename command>
void raft_storage<command>::saveMetaDataToDir(int current_term)  {

  // std::cout << "Begin save meta data to" + file_dir << std::endl;
  std::ofstream out_meta(file_dir + "/meta_data.txt", std::ios::trunc);
  out_meta << current_term ;
  out_meta.close();

  // std::ifstream in_meta(file_dir + "/meta_data.txt");
  // std::string in;
  // std::getline(in_meta, in);
  // std::cout << "Finish save meta data to" + file_dir << "content: " << in
  //           << std::endl;
}

template <typename command>
void raft_storage<command>::saveLogToDir()  {

  // std::cout << "Begin save log to" + file_dir << std::endl;
  std::ofstream out_log(file_dir + "/log.txt", std::ios::trunc);
  out_log << (unsigned int)entryVector.size() << "\t";
  for (unsigned i = 0; i < entryVector.size(); i++)
    out_log << entryVector[i] << "\t";
  out_log.close();

  // std::ifstream in_meta(file_dir + "/log.txt");
  // std::string in;
  // std::getline(in_meta, in);
  // std::cout << "Finish save log to" + file_dir << "content: " << in
  //           << std::endl;
}

template <typename command>
void raft_storage<command>::saveSnapShotToDir()  {

  // std::cout << "Begin snapshot log to" + file_dir << std::endl;
  std::ofstream out_snap(file_dir + "/snapshot.txt", std::ios::trunc);
  out_snap << (unsigned int)snapshot_index << "\n";
  // out_snap << (unsigned int)snapshotData.size() << "\t";
  
  std::string str;
  for(const char& str_char : snapshotData)
    str.push_back(str_char);
  out_snap << str;

  out_snap.close();
}


#endif  // raft_storage_h