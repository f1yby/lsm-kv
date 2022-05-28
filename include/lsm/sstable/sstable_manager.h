#ifndef LSM_KV_SST_MGR_H
#define LSM_KV_SST_MGR_H
#include "sstable.h"
#include "utils.h"
#include <fstream>
#include <list>
namespace kvs {
class SSTMgr {
private:
  std::list<std::vector<SSTable>> _data;
  const std::string _dir;
  void merge();
  void mergeN(const std::vector<SSTable> &l1, std::vector<SSTable> &l2,
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
