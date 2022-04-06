#ifndef MEM_TABLE_SS_TABLE_H
#define MEM_TABLE_SS_TABLE_H

#include "bloom_filter/bloom_filter.hpp"
#include "lsm/sstable/sstable.h"
#include "skip_list/skip_list.hpp"

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

} // namespace kvs

#endif