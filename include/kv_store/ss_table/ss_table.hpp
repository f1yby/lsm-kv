#ifndef SS_TABLE_SS_TABLE_H
#define SS_TABLE_SS_TABLE_H
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
template <typename KeyType, typename ValType> class SSTable {
private:
  typedef SKNode<KeyType, ValType> node_type;
  typedef SkipList<KeyType, ValType> st_type;
  typedef BloomFilter<KeyType> filter_type;

  std::string filepath;
  st_type skip_list;
  filter_type _filter;
  uint32_t size;
  uint32_t key_size_max;
  const uint32_t dump;
  KeyType *minkey;
  KeyType *maxkey;
  static uint64_t table_cnt;
  uint64_t id;

public:
  explicit SSTable(std::string f, uint64_t i = table_cnt,
                   uint32_t d = 2 * 1024 * 1024,
                   uint32_t key_size_max = sizeof(KeyType));
  ~SSTable();

  filter_type filter() const;

  const KeyType *key_min() const;
  const KeyType *key_max() const;

  bool insert(const KeyType &key, const ValType &val);
  ValType *search(const KeyType &key) ;

  void write();
};

template <typename KeyType, typename ValType>
SSTable<KeyType, ValType>::SSTable(std::string f, uint64_t i, uint32_t d,
                                   uint32_t key_size_max)
    : dump(d), filepath(std::move(f)), size(0), skip_list(2), id(i),
      _filter(10240 / 2, key_size_max), key_size_max(key_size_max),
      minkey(nullptr), maxkey(nullptr) {
  size += 32; // header size
  ++table_cnt;
}
template <typename KeyType, typename ValType>
bool SSTable<KeyType, ValType>::insert(const KeyType &key, const ValType &val) {
  ValType *v = skip_list.search(key);
  if (v) {
    bio::bitstream bold;
    bold << *v;
    size -= bold.size();
    bold.clear();
    bold << val;
    size += bold.size();
  } else {
    bio::bitstream bnew;
    bnew << val << key;
    size += bnew.size();
    if (minkey == nullptr || key < *minkey) {
      delete minkey;
      minkey = new KeyType(key);
    }
    if (maxkey == nullptr || *maxkey < key) {
      delete maxkey;
      maxkey = new KeyType(key);
    }
  }
  if (size > dump) {
    write();
    return false;
  } else {
    skip_list.insert(key, val);
    _filter.insert(key);
    return true;
  }
}

template <typename KeyType, typename ValType>
ValType *SSTable<KeyType, ValType>::search(const KeyType &key) {
  if (_filter.check(key)) {
    return skip_list.search(key);
  }
  return nullptr;
}

template <typename KeyType, typename ValType>
BloomFilter<KeyType> SSTable<KeyType, ValType>::filter() const {
  return _filter;
}
template <typename KeyType, typename ValType>
const KeyType *SSTable<KeyType, ValType>::key_min() const {
  return minkey;
}
template <typename KeyType, typename ValType>
const KeyType *SSTable<KeyType, ValType>::key_max() const {
  return maxkey;
}

template <typename KeyType, typename ValType>
void SSTable<KeyType, ValType>::write() {

  //  uint64_t timestamp =
  //      std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
  std::ofstream fout;
  fout.open(filepath);
  bio::bitstream bout;
  bout //<< timestamp
      << id << skip_list.size() << (minkey != nullptr ? *minkey : KeyType())
      << (maxkey != nullptr ? *maxkey : KeyType()) << _filter;
  uint32_t KOffset = bout.size();
  uint32_t VOffset =
      KOffset + (key_size_max + sizeof(KOffset)) * skip_list.size();

  fout << bout;

  for (auto i = skip_list.begin(); i != skip_list.end(); i = i->forwards[0]) {
    fout.seekp(KOffset);
    bout << i->key << VOffset;
    KOffset += bout.size();
    fout << bout;

    fout.seekp(VOffset);
    bout << i->val;
    VOffset += bout.size();
    fout << bout;
  }
  fout.close();
}
template <typename KeyType, typename ValType>
SSTable<KeyType, ValType>::~SSTable() {
  delete minkey;
  delete maxkey;
}

template <typename KeyType, typename ValType>
uint64_t SSTable<KeyType, ValType>::table_cnt = 0;

#endif