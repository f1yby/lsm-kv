#ifndef KV_STORE_SS_TABLE_INDEX
#define KV_STORE_SS_TABLE_INDEX
#include <cstdint>
#include <fstream>
#include <memory>
#include <utility>

#include "bloom_filter/bloom_filter.hpp"
#include "../mem_table/mem_table.hpp"

namespace kvs {
class SSTable;

class SSTableNode {
public:
  uint64_t key{};
  uint32_t offset = 0;
  uint32_t vlen = 0;
  SSTableNode(const uint64_t &k, const uint32_t &o, const uint32_t &l);
  SSTableNode();
};
//
// class SSNodePool {
// private:
//  uint64_t _id;
//  std::list<std::pair<uint64_t, std::string>> table;
//  BloomFilter<uint64_t> filter;
//  enum { key_size_max = 8 };
//  uint32_t data_size;
//
// public:
//  explicit SSNodePool(uint64_t id);
//  void insert(const std::pair<uint64_t, std::string> &kv) {
//    table.push_back(kv);
//    filter.insert(kv.first);
//  }
////  SSTable write(const std::string& filepath) {
////    return SSTable(_id, (table.begin(), table.end()), filter, filepath);
////  }
//};
// SSNodePool::SSNodePool(uint64_t id)
//    : _id(id), table(), filter(10240 / 2, key_size_max), data_size(0){}

class SSTable {
private:
  const uint64_t _id;
  std::vector<SSTableNode> table;
  const BloomFilter<uint64_t> filter;
  enum { key_size_max = 8 };

public:
  std::string filepath;
  SSTable(uint64_t id,
          const std::vector<std::pair<uint64_t, std::string>> &data,
          const BloomFilter<uint64_t> &filter, std::string fp = std::string());

  [[nodiscard]] uint64_t id() const;
  [[nodiscard]] bool check(const uint64_t &k) const;
  [[nodiscard]] uint64_t front() const;
  [[nodiscard]] uint64_t back() const;
  [[nodiscard]] uint32_t size() const;
  bool cover(const SSTable *st) const;
  [[nodiscard]] std::unique_ptr<std::string> get(const SSTableNode &n) const {
    std::ifstream fin;
    fin.open(filepath);
    bio::bitstream bin;
    fin.seekg(n.offset);
    bin.rdnbyte(fin, n.vlen);
    std::unique_ptr<std::string> ans(new std::string());
    bin >> *ans;
    return ans;
  }
  std::unique_ptr<std::string> search(const uint64_t &key) {
    if ((key < front() || back() < key) && !check(key)) {
      return {nullptr};
    }
    for (auto i : table) {
      if (i.key == key) {
        return get(i);
      }
    }
    return {nullptr};
  }
  SSTableNode operator[](uint32_t i);
  void scan(const uint64_t &key1, const uint64_t &key2,
            std::list<std::pair<uint64_t, std::string>> &list) const {
    if (!(key1 > table.back().key || key2 < table.front().key)) {
      // TODO Binary Search
      uint32_t start = 0;
      uint32_t end = table.size() - 1;
      while (table[start].key < key1) {
        ++start;
      }
      while (table[end].key > key2) {
        --end;
      }
      auto j = list.begin();
      while (start <= end) {
        if (j == list.end() || table[start].key < j->first) {
          j = list.insert(j, std::pair<uint64_t, std::string>(
                                 table[start].key, *get(table[start])));
          ++j;
          ++start;
        } else if (table[start].key == j->first) {
          ++start;
          ++j;
        } else {
          ++j;
        }
      }
    }
  }
};

SSTableNode::SSTableNode(const uint64_t &k, const uint32_t &o,
                         const uint32_t &l)
    : key(k), offset(o), vlen(l) {}

SSTableNode::SSTableNode() = default;

uint64_t SSTable::id() const { return _id; }

bool SSTable::check(const uint64_t &k) const {
  return filter.check(k); // Todo Use Binary Search Instead;
}

uint64_t SSTable::front() const { return table.front().key; }

uint64_t SSTable::back() const { return table.back().key; }

bool SSTable::cover(const SSTable *st) const {
  return (front() <= st->front() && back() >= st->front()) ||
         (front() <= st->back() && back() >= st->back());
}

SSTableNode SSTable::operator[](uint32_t i) { return table[i]; }

uint32_t SSTable::size() const { return table.size(); }

SSTable::SSTable(uint64_t id,
                 const std::vector<std::pair<uint64_t, std::string>> &data,
                 const BloomFilter<uint64_t> &filter, std::string fp)
    : _id(id), filter(filter), filepath(std::move(fp)) {
  std::ofstream fout;
  fout.open(filepath);

  bio::bitstream bout;
  bout << _id << data.size() << data.front().first << data.back().first
       << filter;
  uint32_t KOffset = bout.size();
  uint32_t VOffset = KOffset + (key_size_max + sizeof(KOffset)) * data.size();

  fout << bout;

  std::vector<SSTableNode> v(data.size());
  for (uint32_t i = 0; i < data.size(); ++i) {
    fout.seekp(KOffset);
    bout << data[i].first << VOffset;
    KOffset += bout.size();
    fout << bout;
    v[i].key = data[i].first;
    // bad but quick
    fout.seekp(VOffset);
    //    bout << data[i].second;
    fout << data[i].second;
    v[i].vlen = data[i].second.size();
    v[i].offset = VOffset;
    VOffset += bout.size();
    //    v[j].vlen = bout.size();
    //    v[j].offset = VOffset;
    //    VOffset += bout.size();
  }
  fout.close();
  table = v;
}
} // namespace kvs
#endif