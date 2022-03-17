#ifndef SS_TABLE_SS_TABLE_H
#define SS_TABLE_SS_TABLE_H
#include "../bloom_filter/bloom_filter.hpp"
#include "../skip_list/skip_list.hpp"
#include <cstddef>
#include <cstdio>
#include <fstream>
#include <string>
template <typename KeyType, typename ValType> class SSTable {
private:
  const std::string dir;
  std::size_t size;
  SkipList<KeyType, ValType> skip_list;
  BloomFilter<KeyType> filter;

public:
  SSTable(const std::string &dir) : dir(dir) {}
  void write() {
    std::ofstream target;
    target.open(dir);
    target.seekp(0);
  }
};
#endif