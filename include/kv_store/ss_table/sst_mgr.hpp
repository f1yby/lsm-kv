//
// Created by jiarui on 3/18/22.
//

#ifndef LSM_KV_SST_MGR_HPP
#define LSM_KV_SST_MGR_HPP
#include "ss_table.hpp"
#include "utils.h"
#include <list>
#include <utility>
#include <vector>
namespace {
void RecursiveRMDir(const std::string &dir) {
  std::vector<std::string> point;
  utils::scanDir(dir, point);
  for (const auto &i : point) {
    std::vector<std::string> dp;
    utils::scanDir(dir, dp);
    for (const auto &j : dp) {
      auto fp =
          std::string().append(dir).append("/").append(i).append("/").append(j);
      utils::rmfile(fp.c_str());
    }
    utils::rmdir(std::string().append(dir).c_str());
  }
}

} // namespace
namespace kvs {
template <typename KeyType, typename ValType> class SSTMgr {
private:
  std::vector<std::list<SSTable<KeyType, ValType>>> data;
  const std::string dir;
  void merge(uint32_t lvl);
  void mergeN(std::vector<SSTable<KeyType, ValType>> &v);

public:
  SSTMgr();
  explicit SSTMgr(std::string dirpath);
  void insert(const SSTable<KeyType, ValType> &sst);
  std::unique_ptr<ValType> search(const KeyType &k) const;
  void clear();
  void scan(KeyType key1, KeyType key2,
            std::list<std::pair<KeyType, ValType>> &list);
};
template <typename KeyType, typename ValType>
SSTMgr<KeyType, ValType>::SSTMgr() : data(1) {}
template <typename KeyType, typename ValType>
void SSTMgr<KeyType, ValType>::insert(const SSTable<KeyType, ValType> &sst) {
  // Todo Hierarchy
  data[0].push_back(sst);
//  merge();
}
template <typename KeyType, typename ValType>
std::unique_ptr<ValType>
SSTMgr<KeyType, ValType>::search(const KeyType &k) const {
  for (auto j : data) {
    for (SSTable i : j) {
      std::unique_ptr<ValType> ans = i.search(k);
      if (ans != nullptr) {
        return ans;
      }
    }
  }
  return std::unique_ptr<ValType>(nullptr);
}
template <typename KeyType, typename ValType>
void SSTMgr<KeyType, ValType>::clear() {
  data.clear();
  RecursiveRMDir(dir);
}
template <typename KeyType, typename ValType>
SSTMgr<KeyType, ValType>::SSTMgr(std::string dirpath)
    : dir(std::move(dirpath)) {}
template <typename KeyType, typename ValType>
void SSTMgr<KeyType, ValType>::scan(
    KeyType key1, KeyType key2, std::list<std::pair<KeyType, ValType>> &list) {
  for (auto i : data) {
    for (auto j : i) {
      j.scan(key1, key2, list);
    }
  }
}
template <typename KeyType, typename ValType>
void SSTMgr<KeyType, ValType>::merge(uint32_t lvl) {
  if (lvl == 0) {
  }
}
template <typename KeyType, typename ValType>
void SSTMgr<KeyType, ValType>::mergeN(
    std::vector<SSTable<KeyType, ValType>> &v) {
  std::vector<uint32_t> indexs(0, v.size());
  while (true) {
    KeyType k = v[0][indexs[0]];
    uint32_t iadd;
    bool find = false;
    for (int i = 0; i < indexs.size(); ++i) {
      if (indexs[i] < v[i].size() && v[i][indexs[i]] < k) {
        k = v[i][indexs[i]];
        if (!find) {
          find = true;
        }
        ++indexs[iadd];
      }
    }
    if (!find) {
      break;
    }
  }
}

} // namespace kvs
#endif // LSM_KV_SST_MGR_HPP
