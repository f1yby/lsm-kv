#ifndef KV_STORE_SS_TABLE_INDEX
#define KV_STORE_SS_TABLE_INDEX
#include "kv_store/bloom_filter/bloom_filter.hpp"
#include "kv_store/mem_table/mem_table.hpp"
namespace kvs {
template <typename KeyType> class SSTableNode {
public:
  KeyType key;
  uint32_t offset;
  uint32_t vlen;
  SSTableNode(const KeyType &k, const uint32_t &o, const uint32_t &l);
  SSTableNode() = default;
};
template <typename KeyType> class SSTable {
private:
  const uint64_t _id;
  std::vector<SSTableNode<KeyType>> table;
  const BloomFilter<KeyType> filter;

public:
  SSTable(uint64_t id, const std::vector<SSTableNode<KeyType>> &table,
          const BloomFilter<KeyType> &filter);
  ~SSTable() = default;

  [[nodiscard]] uint64_t id() const;
  bool check(const KeyType &k) const;
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
  return filter.check(k); // TODO Use Binary Search Instead;
}
template <typename KeyType>
SSTable<KeyType>::SSTable(const uint64_t id,
                          const std::vector<SSTableNode<KeyType>> &table,
                          const BloomFilter<KeyType> &filter)
    : _id(id), table(table), filter(filter) {}
} // namespace kvs
#endif