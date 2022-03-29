//
// Created by jiarui on 3/18/22.
//

#ifndef LSM_KV_SST_MGR_HPP
#define LSM_KV_SST_MGR_HPP
#include "ss_table.hpp"
#include <list>
#include <vector>
namespace kvs {
template <typename KeyType, typename ValType> class SSTMgr {
private:
  std::vector<std::list<SSTable<KeyType, ValType>>> data;

public:
  SSTMgr();
  void insert(const SSTable<KeyType, ValType> &sst);
  std::unique_ptr<ValType> search(const KeyType &k) const;
  void clear();
  void scan(KeyType key1, KeyType key2,
            std::list<std::pair<KeyType, ValType>> &list) {
    for (auto i : data) {
      for (auto j : i) {
        j.scan(key1, key2, list);
      }
    }
  }
};
template <typename KeyType, typename ValType>
SSTMgr<KeyType, ValType>::SSTMgr() : data(1) {}
template <typename KeyType, typename ValType>
void SSTMgr<KeyType, ValType>::insert(const SSTable<KeyType, ValType> &sst) {
  // Todo Hierarchy
  data[0].push_back(sst);
}
template <typename KeyType, typename ValType>
std::unique_ptr<ValType>
SSTMgr<KeyType, ValType>::search(const KeyType &k) const {
  // Todo Hierarchy
  //  const SSTableNode<KeyType,ValType> *n = nullptr;
  //  const std::string *fp;
  //  auto *target = &(*data[0].begin());
  for (SSTable i : data[0]) {
    std::unique_ptr<ValType> ans = i.search(k);
    if (ans != nullptr) {
      return ans;
    }
  }
  //    if (i.front() <= k && k <= i.back() && i.check(k)) {
  //      auto m = i.search(k);
  //      n = m != nullptr ? m : n;
  //      fp = &(i.filepath);
  //    }
  //
  //  if (n != nullptr) {
  //    std::ifstream fin;
  //    fin.open(*fp);
  //    bio::bitstream bin;
  //    fin.seekg(n->offset);
  //    bin.rdnbyte(fin, n->vlen);
  //    std::unique_ptr<ValType> ans(new ValType());
  //    bin >> *ans;
  //    return ans;
  //  }
  return std::unique_ptr<ValType>(nullptr);
}
template <typename KeyType, typename ValType>
void SSTMgr<KeyType, ValType>::clear() {
  data.clear();
  // TODO Clear the file
}

} // namespace kvs
#endif // LSM_KV_SST_MGR_HPP
