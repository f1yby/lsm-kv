#ifndef MEM_TABLE_SS_TABLE_H
#define MEM_TABLE_SS_TABLE_H

#include "../bitstream.hpp"
#include "../bloom_filter/bloom_filter.hpp"
#include "../skip_list/skip_list.hpp"
#include <chrono>
#include <cstddef>
#include <cstdio>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <string>
#include <utility>
namespace kvs {
template <typename KeyType, typename ValType> class SSTableNode;
template <typename KeyType, typename ValType> class SSTable;

template <typename KeyType, typename ValType> class MemTable {
private:
  typedef SKNode<KeyType, ValType> node_type;
  typedef SkipList<KeyType, ValType> st_type;
  typedef BloomFilter<KeyType> filter_type;

  st_type skip_list;
  filter_type _filter;
  uint32_t data_size;
  uint32_t key_size_max;
  const uint32_t dump;
  KeyType *minkey;
  KeyType *maxkey;
  static uint64_t table_cnt;
  uint64_t _id;

public:
  explicit MemTable(uint64_t i = table_cnt, uint32_t d = 2 * 1024 * 1024,
                    uint32_t key_size_max = sizeof(KeyType));
  MemTable(const MemTable &) = delete;
  MemTable &operator=(const MemTable &) = delete;
  MemTable(MemTable &&) = delete;
  MemTable &operator=(MemTable &&) = delete;
  ~MemTable();

  filter_type filter() const;
  const KeyType *key_min() const;
  const KeyType *key_max() const;
  [[nodiscard]] uint64_t size() const;
  [[nodiscard]] uint64_t id() const;

  bool insert(const KeyType &key, const ValType &val);
  ValType *search(const KeyType &key) const;
  std::list<std::pair<KeyType, ValType>> scan(const KeyType &start,
                                              const KeyType &end) const;

  [[nodiscard]] SSTable<KeyType, ValType>
  write(const std::string &filepath) const;
  void remove(const KeyType &key) { skip_list.remove(key); }
};

template <typename KeyType, typename ValType>
MemTable<KeyType, ValType>::MemTable(uint64_t id, uint32_t dumplimit,
                                     uint32_t key_size_max)
    : skip_list(2), _filter(10240 / 2, key_size_max), data_size(0),
      key_size_max(key_size_max), dump(dumplimit), minkey(nullptr),
      maxkey(nullptr), _id(id) {
  data_size += 32; // header size
  ++table_cnt;
}
template <typename KeyType, typename ValType>
bool MemTable<KeyType, ValType>::insert(const KeyType &key,
                                        const ValType &val) {
  ValType *v = skip_list.search(key);
  if (v) {
    bio::bitstream bold;
    bold << *v;
    data_size -= bold.size();
    bold.clear();
    bold << val;
    data_size += bold.size();
  } else {
    bio::bitstream bnew;
    bnew << val << key;
    data_size += bnew.size();
    if (minkey == nullptr || key < *minkey) {
      delete minkey;
      minkey = new KeyType(key);
    }
    if (maxkey == nullptr || *maxkey < key) {
      delete maxkey;
      maxkey = new KeyType(key);
    }
  }
  if (data_size > dump) {
    return false;
  } else {
    skip_list.insert(key, val);
    _filter.insert(key);
    return true;
  }
}

template <typename KeyType, typename ValType>
ValType *MemTable<KeyType, ValType>::search(const KeyType &key) const {
  if (_filter.check(key)) {
    return skip_list.search(key);
  }
  return nullptr;
}

template <typename KeyType, typename ValType>
BloomFilter<KeyType> MemTable<KeyType, ValType>::filter() const {
  return _filter;
}
template <typename KeyType, typename ValType>
const KeyType *MemTable<KeyType, ValType>::key_min() const {
  return minkey;
}
template <typename KeyType, typename ValType>
const KeyType *MemTable<KeyType, ValType>::key_max() const {
  return maxkey;
}

template <typename KeyType, typename ValType>
SSTable<KeyType, ValType>

MemTable<KeyType, ValType>::write(const std::string &filepath) const {
  std::vector<std::pair<KeyType, ValType>> v(skip_list.size());
  int j = 0;
  for (auto i = skip_list.begin(); i != skip_list.end();
       i = i->forwards[0], ++j) {
    v[j].first = i->key;
    v[j].second = i->val;
  }
  return SSTable<KeyType, ValType>(id(), v, filter(), filepath);
}
template <typename KeyType, typename ValType>
MemTable<KeyType, ValType>::~MemTable() {
  delete minkey;
  delete maxkey;
}
template <typename KeyType, typename ValType>
uint64_t MemTable<KeyType, ValType>::size() const {
  return skip_list.size();
}
template <typename KeyType, typename ValType>
std::list<std::pair<KeyType, ValType>>
MemTable<KeyType, ValType>::scan(const KeyType &start,
                                 const KeyType &end) const {
  std::list<std::pair<KeyType, ValType>> ans;
  for (auto i : skip_list.scan(start, end)) {
    ans.push_back(std::pair<KeyType, ValType>(i->key, i->val));
  }
  return ans;
}
template <typename KeyType, typename ValType>
[[maybe_unused]] uint64_t MemTable<KeyType, ValType>::id() const {
  return _id;
}

template <typename KeyType, typename ValType>
uint64_t MemTable<KeyType, ValType>::table_cnt = 0;
} // namespace kvs

#endif