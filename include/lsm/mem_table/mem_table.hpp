#ifndef MEM_TABLE_SS_TABLE_H
#define MEM_TABLE_SS_TABLE_H

#include "../bitstream.hpp"
#include "../bloom_filter/bloom_filter.hpp"
#include "../skip_list/skip_list.hpp"
#include "../ss_table/ss_table.hpp"
#include <chrono>
#include <cstddef>
#include <cstdio>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <string>
#include <utility>
using node_type= SKNode<uint64_t ,std::string >;
typedef SkipList<uint64_t ,std::string>st_type;
typedef BloomFilter<uint64_t>filter_type;
namespace kvs {

//class SSTableNode;
//class SSTable;

class MemTable {
private:


  st_type skip_list;
  filter_type _filter;
  uint32_t data_size;
  const uint32_t dump;
  uint64_t *minkey;
  uint64_t *maxkey;
  static uint64_t table_cnt;
  uint64_t _id;
  enum { key_size_max = 8 };

public:
  explicit MemTable(uint64_t i = table_cnt, uint32_t d = 2 * 1024 * 1024);
  MemTable(const MemTable &) = delete;
  MemTable &operator=(const MemTable &) = delete;
  MemTable(MemTable &&) = delete;
  MemTable &operator=(MemTable &&) = delete;
  ~MemTable();

  [[nodiscard]] filter_type filter() const;
  [[nodiscard]] const uint64_t *key_min() const;
  [[nodiscard]] const uint64_t *key_max() const;
  [[nodiscard]] uint64_t size() const;
  [[nodiscard]] uint64_t id() const;

  bool insert(const uint64_t &key, const std::string &val);
  [[nodiscard]] std::string *search(const uint64_t &key) const;
  [[nodiscard]] std::list<std::pair<uint64_t, std::string>> scan(const uint64_t &start,
                                                   const uint64_t &end) const;

  [[nodiscard]] SSTable write(const std::string &filepath) const;
  void remove(const uint64_t &key) { skip_list.remove(key); }
};

MemTable::MemTable(uint64_t id, uint32_t dumplimit)
    : skip_list(2), _filter(10240 / 2, key_size_max), data_size(0),dump(dumplimit), minkey(nullptr),
      maxkey(nullptr), _id(id) {
  data_size += 32; // header size
  ++table_cnt;
}
bool MemTable::insert(const uint64_t &key,
                                        const std::string &val) {
  std::string *v = skip_list.search(key);
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
      minkey = new uint64_t (key);
    }
    if (maxkey == nullptr || *maxkey < key) {
      delete maxkey;
      maxkey = new uint64_t (key);
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

std::string *MemTable::search(const uint64_t &key) const {
  if (_filter.check(key)) {
    return skip_list.search(key);
  }
  return nullptr;
}

filter_type MemTable::filter() const {
  return _filter;
}

const uint64_t *MemTable::key_min() const {
  return minkey;
}

const uint64_t *MemTable::key_max() const {
  return maxkey;
}


SSTable MemTable::write(const std::string &filepath) const {
  std::vector<std::pair<uint64_t , std::string>> v(skip_list.size());
  int j = 0;
  for (auto i = skip_list.begin(); i != skip_list.end();
       i = i->forwards[0], ++j) {
    v[j].first = i->key;
    v[j].second = i->val;
  }
  return SSTable(id(), v, filter(), filepath);
}

MemTable::~MemTable() {
  delete minkey;
  delete maxkey;
}

uint64_t MemTable::size() const {
  return skip_list.size();
}
std::list<std::pair<uint64_t, std::string>>
MemTable::scan(const uint64_t&start,
                                 const uint64_t &end) const {
  std::list<std::pair<uint64_t, std::string>> ans;
  for (auto i : skip_list.scan(start, end)) {
    ans.emplace_back(i->key, i->val);
  }
  return ans;
}
[[maybe_unused]] uint64_t MemTable::id() const {
  return _id;
}


uint64_t MemTable::table_cnt = 0;
} // namespace kvs

#endif