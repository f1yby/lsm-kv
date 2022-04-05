#ifndef LSM_KV_SST_MGR_H
#define LSM_KV_SST_MGR_H
#include <list>
#include <fstream>
#include "sstable.h"
#include "utils.h"
namespace kvs {
class SSTMgr {
private:
  std::list<std::list<SSTable>> _data;
  const std::string _dir;
  void merge();
  void merge1(const std::list<SSTable> &l1, std::list<SSTable> &l2);
  void mergeN(const std::list<SSTable> &l1, std::list<SSTable> &l2,
              uint64_t lvl);
public:
  SSTMgr();
  explicit SSTMgr(std::string dir);
  void insert(const SSTable &sst);
  [[nodiscard]] std::unique_ptr<std::string> search(const uint64_t &k) const;
  void clear();
  void scan(const uint64_t &key1, const uint64_t &key2,
            std::list<std::pair<uint64_t, std::string>> &list) const;
};

} // namespace kvs
#endif // LSM_KV_SST_MGR_H
