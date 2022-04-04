//
// Created by jiarui on 3/18/22.
//

#ifndef LSM_KV_SST_MGR_H
#define LSM_KV_SST_MGR_H
#include "../mem_table/mem_table.hpp"
#include "ss_table.h"
#include "utils.h"
#include <cstdint>
#include <list>
#include <utility>
#include <vector>
#include "utils/filesys.h"

namespace kvs {
class SSTMgr {
private:
  std::vector<std::list<SSTable>> data;
  const std::string dir;
  void merge();

public:
  SSTMgr();
  explicit SSTMgr(std::string dirpath);
  void insert(const SSTable &sst);
  [[nodiscard]] std::unique_ptr<std::string> search(const uint64_t &k) const;
  void clear();
  void scan(const uint64_t &key1, const uint64_t &key2,
            std::list<std::pair<uint64_t , std::string>> &list) const;
};


SSTMgr::SSTMgr() : data(1) {}

void SSTMgr::insert(const SSTable &sst) {
  // Todo Hierarchy
  data[0].push_back(sst);
  // merge();
}

std::unique_ptr<std::string>
SSTMgr::search(const uint64_t &k) const {
  for (const auto& j : data) {
    for (SSTable i : j) {
      std::unique_ptr<std::string> ans = i.search(k);
      if (ans != nullptr) {
        return ans;
      }
    }
  }
  return {nullptr};
}

void SSTMgr::clear() {
  data.clear();
  RecursiveRMDir(dir);
}

SSTMgr::SSTMgr(std::string dirpath)
    : dir(std::move(dirpath)) {}

void SSTMgr::scan(
    const uint64_t &key1, const uint64_t &key2,
    std::list<std::pair<uint64_t , std::string>> &list) const {
  for (const auto& i : data) {
    for (const auto &j : i) {
      j.scan(key1, key2, list);
    }
  }
}

void SSTMgr::merge() {
  for (uint32_t i = 0; i < data.size(); ++i) {
    uint64_t size = data[i].size();
    if (data[i].size() > i * 2 + 2) {
      size = i * 2 + 2 - size;
      std::vector<SSTableNode> v;
    }
  }
}
} // namespace kvs
#endif // LSM_KV_SST_MGR_H
