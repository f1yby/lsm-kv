#ifndef MEM_TABLE_SS_TABLE_H
#define MEM_TABLE_SS_TABLE_H

#include "bloom_filter/bloom_filter.hpp"
#include "lsm/sstable/sstable.h"
#include "skip_list/skip_list.hpp"
#include "utils/bitstream.h"
#include <chrono>
#include <cstddef>
#include <cstdio>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <string>
#include <utility>

using SkipList_t = SkipList<uint64_t, std::string>;
using Filter_t = BloomFilter<uint64_t>;
namespace kvs {

class MemTable {
private:
  SkipList_t _skip_list;
  Filter_t _filter;
  uint32_t data_size;
  const uint32_t dump;
  uint64_t *_key_min;
  uint64_t *_key_max;
  uint64_t _id;
  enum { key_size_max = 8 };

public:
  explicit MemTable(uint64_t i, uint32_t dump_limit = 2 * 1024 * 1024);
  MemTable(const MemTable &) = delete;
  MemTable &operator=(const MemTable &) = delete;
  MemTable(MemTable &&) = delete;
  MemTable &operator=(MemTable &&) = delete;
  ~MemTable();

  [[nodiscard]] Filter_t filter() const;
  [[nodiscard]] const uint64_t *key_min() const;
  [[nodiscard]] const uint64_t *key_max() const;
  [[nodiscard]] uint64_t size() const;
  [[nodiscard]] uint64_t id() const;

  bool insert(const uint64_t &key, const std::string &val);
  [[nodiscard]] std::string *search(const uint64_t &key) const;
  [[nodiscard]] std::list<std::pair<uint64_t, std::string>>
  scan(const uint64_t &start, const uint64_t &end) const;

  [[nodiscard]] SSTable write(const std::string &filepath) const;
  void remove(const uint64_t &key) { _skip_list.remove(key); }
};

MemTable::MemTable(uint64_t id, uint32_t dump_limit)
    : _skip_list(2), _filter(10240 / 2, key_size_max), data_size(0),
      dump(dump_limit), _key_min(nullptr), _key_max(nullptr), _id(id) {
  data_size += 32; // header size
}
bool MemTable::insert(const uint64_t &key, const std::string &val) {
  std::string *v = _skip_list.search(key);
  if (v) {
    bio::bitstream bold;
    bold << *v;
    data_size -= bold.size();
    bold.clear();
    bold << val;
    data_size += bold.size();
  } else {
    bio::bitstream bNew;
    bNew << val << key;
    data_size += bNew.size();
    if (_key_min == nullptr || key < *_key_min) {
      delete _key_min;
      _key_min = new uint64_t(key);
    }
    if (_key_max == nullptr || *_key_max < key) {
      delete _key_max;
      _key_max = new uint64_t(key);
    }
  }
  if (data_size > dump) {
    return false;
  } else {
    _skip_list.insert(key, val);
    _filter.insert(key);
    return true;
  }
}

std::string *MemTable::search(const uint64_t &key) const {
  if (_filter.check(key)) {
    return _skip_list.search(key);
  }
  return nullptr;
}

Filter_t MemTable::filter() const { return _filter; }

const uint64_t *MemTable::key_min() const { return _key_min; }

const uint64_t *MemTable::key_max() const { return _key_max; }

SSTable MemTable::write(const std::string &filepath) const {
  std::vector<std::pair<uint64_t, std::string>> v(_skip_list.size());
  int j = 0;
  for (auto i = _skip_list.begin(); i != _skip_list.end();
       i = i->forwards[0], ++j) {
    v[j].first = i->key;
    v[j].second = i->val;
  }
  return {id(), v, filter(), filepath};
}

MemTable::~MemTable() {
  delete _key_min;
  delete _key_max;
}

uint64_t MemTable::size() const { return _skip_list.size(); }
std::list<std::pair<uint64_t, std::string>>
MemTable::scan(const uint64_t &start, const uint64_t &end) const {
  std::list<std::pair<uint64_t, std::string>> ans;
  for (auto i : _skip_list.scan(start, end)) {
    ans.emplace_back(i->key, i->val);
  }
  return ans;
}
[[maybe_unused]] uint64_t MemTable::id() const { return _id; }

} // namespace kvs

#endif