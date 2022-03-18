#ifndef KV_STORE_SS_TABLE_INDEX
#define KV_STORE_SS_TABLE_INDEX
#include <utility>

#include "kv_store/bloom_filter/bloom_filter.hpp"
#include "kv_store/mem_table/mem_table.hpp"
namespace kvs {
template <typename KeyType> class SSTableNode {
public:
  KeyType key;
  uint32_t offset{};
  uint32_t vlen{};
  SSTableNode(const KeyType &k, const uint32_t &o, const uint32_t &l);
  SSTableNode() = default;
};
template <typename KeyType> class SSTable {
private:
  const uint64_t _id;
  std::vector<SSTableNode<KeyType>> table;
  const BloomFilter<KeyType> filter;

public:
  std::string filepath;
  SSTable(uint64_t id, const std::vector<SSTableNode<KeyType>> &table,
          const BloomFilter<KeyType> &filter, std::string fp = std::string());
  ~SSTable() = default;

  [[nodiscard]] uint64_t id() const;
  bool check(const KeyType &k) const;
  SSTableNode<KeyType> *search(const KeyType &k) const;
  KeyType front() const;
  KeyType back() const;
};

template <typename KeyType>
SSTableNode<KeyType>::SSTableNode(const KeyType &k, const uint32_t &o,
                                  const uint32_t &l)
    : key(k), offset(o), vlen(l) {}

template <typename KeyType> uint64_t SSTable<KeyType>::id() const {
  return _id;
}
template <typename KeyType>
bool SSTable<KeyType>::check(const KeyType &k) const {
  return filter.check(k); // Todo Use Binary Search Instead;
}
template <typename KeyType>
SSTable<KeyType>::SSTable(const uint64_t id,
                          const std::vector<SSTableNode<KeyType>> &table,
                          const BloomFilter<KeyType> &filter, std::string fp)
    : _id(id), table(table), filter(filter), filepath(std::move(fp)) {}
template <typename KeyType> KeyType SSTable<KeyType>::front() const {
  return table.front().key;
}
template <typename KeyType> KeyType SSTable<KeyType>::back() const {
  return table.back().key;
}
template <typename KeyType>
SSTableNode<KeyType> *SSTable<KeyType>::search(const KeyType &k) const {
  // TODO Binary Search
  SSTableNode<KeyType> *ans = nullptr;
  for (auto i : table) {
    if (i.key == k) {
      ans = &i;
    }
  }
  return ans;
}
} // namespace kvs
#endif