#include "lsm/lsm.h"
#include "utils/filesys.h"
Lsm::Lsm(const std::string &dir)
    : KVStoreAPI(dir), dir(dir), mTable(new kvs::MemTable(0)), sst_mgr(dir) {
  utils::mkdir(dir.c_str());
}
void Lsm::put(uint64_t key, const std::string &s) {
  if (!mTable->insert(key, s)) {
    dump(touch(std::string().append(dir).append("/").append("level-").append(
                   std::to_string(0)),
               std::to_string(mTable->id()), "sst"));
    mTable->insert(key, s);
  }
}
std::string Lsm::get(uint64_t key) {
  auto mem_ans = mTable->search(key);
  if (mem_ans != nullptr) {
    return *mem_ans == "~DELETED~" ? std::string() : *mem_ans;
  } else {
    auto p = sst_mgr.search(key);
    return p == nullptr || *p == "~DELETED~" ? std::string() : *p;
  }
}
bool Lsm::del(uint64_t key) {
  auto pm = mTable->search(key);
  if (pm != nullptr) {
    if (*pm != "~DELETED~") {
      mTable->remove(key);
      auto ps = sst_mgr.search(key);
      if (ps != nullptr && *ps != "~DELETED~") {
        put(key, "~DELETED~");
      }
      return true;
    }
    return false;
  }
  auto ps = sst_mgr.search(key);
  if (ps != nullptr && *ps != "~DELETED~") {
    put(key, "~DELETED~");
    return true;
  }
  return false;
}
void Lsm::reset() {
  delete mTable;
  mTable = new kvs::MemTable(0);
  sst_mgr.clear();
}
void Lsm::scan(uint64_t key1, uint64_t key2,
               std::list<std::pair<uint64_t, std::string>> &list) {
  std::list<std::pair<uint64_t, std::string>> ml = mTable->scan(key1, key2);
  sst_mgr.scan(key1, key2, ml);
  list = ml;
  for (auto i = list.begin(); i != list.end(); ++i) {
    if (i->second == "~DELETE~") {
      i = list.erase(i);
    }
  }
}
void Lsm::dump(const std::string &filepath) {
  sst_mgr.insert(mTable->write(std::string().append(filepath)));
  auto new_table = new kvs::MemTable(mTable->id() + 1);
  delete mTable;
  mTable = new_table;
}
Lsm::~Lsm() {
  delete mTable;
}
