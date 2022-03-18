#ifndef KV_STORE_SS_TABLE_INDEX
#define KV_STORE_SS_TABLE_INDEX
#include "../bloom_filter/bloom_filter.hpp"
#include "./ss_table.hpp"
namespace sst {
template <typename KeyType, typename ValType> class index {
private:
  const uint64_t id;
  const KeyType *minkey;
  const KeyType *maxkey;
  const std::vector<KeyType> table;
  const BloomFilter<KeyType> filter;

public:
  explicit index(SSTable<KeyType, ValType> s)
      : id(s.id), filter(s.filter()), table(std::vector<KeyType>()),
        minkey(s.key_min() != nullptr ? new KeyType(s.key_min()) : nullptr),
        maxkey(s.key_max() != nullptr ? new KeyType(s.key_max()) : nullptr){};
};
} // namespace sst
#endif