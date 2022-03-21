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
  std::vector<std::list<SSTable<KeyType>>> data;

public:
  SSTMgr();
  void insert(const SSTable<KeyType> &sst);
  std::unique_ptr<ValType> search(const KeyType &k) const;
  void clear();
};
template <typename KeyType, typename ValType>
SSTMgr<KeyType, ValType>::SSTMgr() : data(1) {}
template <typename KeyType, typename ValType>
void SSTMgr<KeyType, ValType>::insert(const SSTable<KeyType> &sst) {
  // Todo Hierarchy
  data[0].push_back(sst);
}
template <typename KeyType, typename ValType>
std::unique_ptr<ValType>
SSTMgr<KeyType, ValType>::search(const KeyType &k) const {
  // Todo Hierarchy
  const SSTableNode<KeyType> *n = nullptr;
  const std::string *fp;
  auto *target = &(*data[0].begin());
  for (auto &i : data[0]) {
    if (i.front() <= k && k <= i.back() && i.check(k)) {
      auto m = i.search(k);
      n = m != nullptr ? m : n;
      fp = &(i.filepath);
    }
  }
  if (n != nullptr) {
    std::ifstream fin;
    fin.open(*fp);
    bio::bitstream bin;
    fin.seekg(n->offset);
    bin.rdnbyte(fin, n->vlen);
    std::unique_ptr<ValType> ans(new ValType());
    bin >> *ans;
    return ans;
  }
  return std::unique_ptr<ValType>(nullptr);
}
template <typename KeyType, typename ValType>
void SSTMgr<KeyType, ValType>::clear() {
  data.clear();
  // TODO Clear the file
}

} // namespace kvs
#endif // LSM_KV_SST_MGR_HPP
