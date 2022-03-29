#ifndef KV_STORE_SS_TABLE_INDEX
#define KV_STORE_SS_TABLE_INDEX
#include <utility>

#include "kv_store/bloom_filter/bloom_filter.hpp"
#include "kv_store/mem_table/mem_table.hpp"
namespace kvs {
template <typename KeyType, typename ValType> class SSTableNode {
public:
  KeyType key;
  uint32_t offset = 0;
  uint32_t vlen = 0;
  SSTableNode(const KeyType &k, const uint32_t &o, const uint32_t &l);
  SSTableNode();
};
template <typename KeyType, typename ValType> class SSTable {
private:
  const uint64_t _id;
  std::vector<SSTableNode<KeyType, ValType>> table;
  const BloomFilter<KeyType> filter;

public:
  std::string filepath;
  SSTable(uint64_t id, const std::vector<SSTableNode<KeyType, ValType>> &table,
          const BloomFilter<KeyType> &filter, std::string fp = std::string());
  ~SSTable() = default;

  [[nodiscard]] uint64_t id() const;
  bool check(const KeyType &k) const;
  // const SSTableNode<KeyType, ValType> *search(const KeyType &k) const;
  KeyType front() const;
  KeyType back() const;
  bool cover(const SSTable *st);
  std::unique_ptr<ValType> search(const KeyType &key) {
    if ((key < front() || back() < key) && !check(key)) {
      return std::unique_ptr<ValType>(nullptr);
    }
    for (SSTableNode i : table) {
      if (i.key == key) {
        std::ifstream fin;
        fin.open(filepath);
        bio::bitstream bin;
        fin.seekg(i.offset);
        bin.rdnbyte(fin, i.vlen);
        std::unique_ptr<ValType> ans(new ValType());
        bin >> *ans;
        return ans;
      }
    }
    return std::unique_ptr<ValType>(nullptr);
  }
  void scan(KeyType key1, KeyType key2,
            std::list<std::pair<KeyType, ValType>> &list);
};

template <typename KeyType, typename ValType>
SSTableNode<KeyType, ValType>::SSTableNode(const KeyType &k, const uint32_t &o,
                                           const uint32_t &l)
    : key(k), offset(o), vlen(l) {}
template <typename KeyType, typename ValType>
SSTableNode<KeyType, ValType>::SSTableNode() : key{} {}

template <typename KeyType, typename ValType>
uint64_t SSTable<KeyType, ValType>::id() const {
  return _id;
}
template <typename KeyType, typename ValType>
bool SSTable<KeyType, ValType>::check(const KeyType &k) const {
  return filter.check(k); // Todo Use Binary Search Instead;
}
template <typename KeyType, typename ValType>
SSTable<KeyType, ValType>::SSTable(
    const uint64_t id, const std::vector<SSTableNode<KeyType, ValType>> &table,
    const BloomFilter<KeyType> &filter, std::string fp)
    : _id(id), table(table), filter(filter), filepath(std::move(fp)) {}
template <typename KeyType, typename ValType>
KeyType SSTable<KeyType, ValType>::front() const {
  return table.front().key;
}
template <typename KeyType, typename ValType>
KeyType SSTable<KeyType, ValType>::back() const {
  return table.back().key;
}
// template <typename KeyType, typename ValType>
// const SSTableNode<KeyType, ValType> *
// SSTable<KeyType, ValType>::search(const KeyType &k) const {
//   // TODO Binary Search
//   const SSTableNode<KeyType, ValType> *ans = nullptr;
//   for (int i = 0; i < table.size(); ++i) {
//     if (table[i].key == k) {
//       ans = &table[i];
//     }
//   }
//   return ans;
// }
template <typename KeyType, typename ValType>
bool SSTable<KeyType, ValType>::cover(const SSTable *st) {
  return front() <= st->front() && back() >= st->front() ||
         front() <= st->back() && back() >= st->back();
}
} // namespace kvs
#endif