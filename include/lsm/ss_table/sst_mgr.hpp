//
// Created by jiarui on 3/18/22.
//

#ifndef LSM_KV_SST_MGR_HPP
#define LSM_KV_SST_MGR_HPP
#include "../mem_table/mem_table.hpp"
#include "ss_table.hpp"
#include "utils.h"
#include <cstdint>
#include <list>
#include <utility>
#include <vector>

inline void RecursiveRMDir(const std::string &dir) {
  std::vector<std::string> points;
  utils::scanDir(dir, points);
  for (const auto &i : points) {

    auto fp = std::string().append(dir).append("/").append(i);
    if (utils::rmfile(fp.c_str())) {
      RecursiveRMDir(fp);
    }
  }
  utils::rmdir(dir.c_str());
}
namespace kvs {
class SSTMgr {
private:
  std::vector<std::list<SSTable>> data;
  const std::string dir;
  void merge();
//  void mergeN(std::vector<SSTable<uint64_t , std::string>> &v);

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
//void SSTMgr::mergeN(
//    std::vector<SSTable> &v) {
//  std::vector<uint32_t> indexs(0, v.size());
//  while (true) {
//    uint64_t k = v[0][indexs[0]];
//    uint32_t iadd;
//    bool find = false;
//    for (int i = 0; i < indexs.size(); ++i) {
//      if (indexs[i] < v[i].size() && v[i][indexs[i]] < k) {
//        k = v[i][indexs[i]];
//        if (!find) {
//          find = true;
//        }
//        ++indexs[iadd];
//      }
//    }
//    if (!find) {
//      break;
//    }
//  }
//}

} // namespace kvs
#endif // LSM_KV_SST_MGR_HPP
