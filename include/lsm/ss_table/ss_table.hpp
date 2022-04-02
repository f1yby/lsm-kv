#ifndef KV_STORE_SS_TABLE_INDEX
#define KV_STORE_SS_TABLE_INDEX
#include <cstdint>
#include <utility>

#include "../bloom_filter/bloom_filter.hpp"
#include "../mem_table/mem_table.hpp"

namespace kvs {
template <typename KeyType, typename ValType> class SSTable;

template <typename KeyType, typename ValType> class SSTableNode {
public:
  KeyType key;
  uint32_t offset = 0;
  uint32_t vlen = 0;
  SSTableNode(const KeyType &k, const uint32_t &o, const uint32_t &l);
  SSTableNode();
};

template <typename KeyType, typename ValType> class SSNodePool {
private:
  uint64_t _id;
  std::list<SSTableNode<KeyType, ValType>> table;
  BloomFilter<KeyType> filter;
  uint32_t key_size_max;
  uint32_t data_size;

public:
  SSNodePool(uint64_t id, uint32_t key_size_max);
  void insert(std::pair<KeyType, ValType> kv) {
    table.push_back(kv);
    filter.insert(kv.first);
  }
  SSTable<KeyType, ValType> write(std::string filepath) {
    return SSTable<KeyType, ValType>(_id, (table.begin(), table.end()), filter,
                                     filepath, key_size_max);
  }
};
template <typename KeyType, typename ValType>
SSNodePool<KeyType, ValType>::SSNodePool(uint64_t id, uint32_t key_size_max)
    : _id(id), table(), filter(10240 / 2, key_size_max), data_size(0),
      key_size_max(key_size_max) {}
template <typename KeyType, typename ValType> class SSTable {
private:
  const uint64_t _id;
  std::vector<SSTableNode<KeyType, ValType>> table;
  const BloomFilter<KeyType> filter;

public:
  std::string filepath;
  SSTable(uint64_t id, const std::vector<std::pair<KeyType, ValType>> &data,
          const BloomFilter<KeyType> &filter, std::string fp = std::string(),
          uint32_t key_size_max = sizeof(KeyType));

  [[nodiscard]] uint64_t id() const;
  bool check(const KeyType &k) const;
  // const SSTableNode<KeyType, ValType> *search(const KeyType &k) const;
  KeyType front() const;
  KeyType back() const;
  [[nodiscard]] uint32_t size() const;
  bool cover(const SSTable *st) const;
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
  SSTableNode<KeyType, ValType> operator[](uint32_t i);
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
KeyType SSTable<KeyType, ValType>::front() const {
  return table.front().key;
}
template <typename KeyType, typename ValType>
KeyType SSTable<KeyType, ValType>::back() const {
  return table.back().key;
}

template <typename KeyType, typename ValType>
bool SSTable<KeyType, ValType>::cover(const SSTable *st) const {
  return (front() <= st->front() && back() >= st->front()) ||
         (front() <= st->back() && back() >= st->back());
}
template <typename KeyType, typename ValType>
SSTableNode<KeyType, ValType>
SSTable<KeyType, ValType>::operator[](uint32_t i) {
  return table[i];
}
template <typename KeyType, typename ValType>
uint32_t SSTable<KeyType, ValType>::size() const {
  return table.size();
}
template <typename KeyType, typename ValType>
SSTable<KeyType, ValType>::SSTable(
    uint64_t id, const std::vector<std::pair<KeyType, ValType>> &data,
    const BloomFilter<KeyType> &filter, std::string fp, uint32_t key_size_max)
    : _id(id), filter(filter), filepath(std::move(fp)) {
  std::ofstream fout;
  fout.open(filepath);

  bio::bitstream bout;
  bout << _id << data.size() << data.front().first << data.back().first
       << filter;
  uint32_t KOffset = bout.size();
  uint32_t VOffset = KOffset + (key_size_max + sizeof(KOffset)) * data.size();

  fout << bout;

  std::vector<SSTableNode<KeyType, ValType>> v(data.size());
  int j = 0;
  for (uint32_t i = 0; i < data.size(); ++i) {
    fout.seekp(KOffset);
    bout << data[i].first << VOffset;
    KOffset += bout.size();
    fout << bout;
    v[j].key = data[i].first;

    fout.seekp(VOffset);
    bout << data[i].second;
    v[j].vlen = bout.size();
    v[j].offset = VOffset;
    VOffset += bout.size();
    fout << bout;
  }
  fout.close();
  table = v;
}
} // namespace kvs
#endif