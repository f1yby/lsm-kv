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
  std::string filepath;
  SkipList<KeyType, ValType> skip_list;
  BloomFilter<KeyType> filter;
  uint32_t size;
  uint32_t key_size_max;
  const uint32_t dump;
  KeyType *minkey;
  KeyType *maxkey;

public:
  explicit SSTable(std::string f, uint32_t d = 2 * 1024 * 1024,
                   uint32_t key_size_max = sizeof(KeyType));
  bool insert(const KeyType &key, const ValType &val);
  ValType *search(const KeyType &key);
  ValType *scan(const KeyType &key);

  void write();
};

template <typename KeyType, typename ValType>
SSTable<KeyType, ValType>::SSTable(std::string f, uint32_t d,
                                   uint32_t key_size_max)
    : dump(d), filepath(std::move(f)), size(0), skip_list(2),
      filter(10240, key_size_max), key_size_max(key_size_max), minkey(nullptr),
      maxkey(nullptr) {
  size += 32; // header size
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
    filter.insert(key);
    return true;
  }
}
template <typename KeyType, typename ValType>
void SSTable<KeyType, ValType>::write() {
  uint64_t timestamp =
      std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
  std::ofstream fout;
  fout.open(filepath);
  bio::bitstream bout;
  bout << timestamp << skip_list.size()
       << (minkey != nullptr ? *minkey : KeyType())
       << (maxkey != nullptr ? *maxkey : KeyType());
  fout << bout;
  uint32_t Koffset = 32;
  uint32_t Voffset = 32 + (key_size_max + sizeof(Koffset)) * skip_list.size();

  for (auto i = skip_list.begin(); i != skip_list.end(); i = i->forwards[0]) {
    fout.seekp(Koffset);
    bout << i->key;
    bout << Voffset;
    Koffset += bout.size();
    fout << bout;

    fout.seekp(Voffset);
    bout << i->val;
    Voffset += bout.size();
    fout << bout;
  }
  fout.close();
}
template <typename KeyType, typename ValType>
ValType *SSTable<KeyType, ValType>::search(const KeyType &key) {
  if (filter.check(key)) {
    return skip_list.search(key);
  }
  return nullptr;
}
template <typename KeyType, typename ValType>
ValType *SSTable<KeyType, ValType>::scan(const KeyType &key) {
  return nullptr;
}

#endif