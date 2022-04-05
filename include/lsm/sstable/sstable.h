#ifndef KV_STORE_SS_TABLE_INDEX
#define KV_STORE_SS_TABLE_INDEX
#include "bloom_filter/bloom_filter.hpp"
#include <cstdint>
#include <fstream>
#include <memory>
#include <utility>
using Filter_t = BloomFilter<uint64_t>;
namespace kvs {

class SSTableNode {
public:
  uint64_t key = 0;
  uint32_t offset = 0;
  uint32_t value_len = 0;
  SSTableNode(const uint64_t &k, const uint32_t &o, const uint32_t &l);
  SSTableNode();
};

class SSTable {
private:
  uint64_t _id;
  std::vector<SSTableNode> table;
  BloomFilter<uint64_t> filter;
  enum { key_size_max = 8 };

public:
  std::string filepath;
  SSTable(uint64_t id,
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
      v[i].value_len = data[i].second.size();
      v[i].offset = VOffset;
      fout.seekp(VOffset);
      fout << data[i].second;

      VOffset += data[i].second.size();
    }
    fout.close();
    table = v;
  }

  [[nodiscard]] uint64_t id() const;
  [[nodiscard]] bool check(const uint64_t &k) const;
  [[nodiscard]] uint64_t front() const;
  [[nodiscard]] uint64_t back() const;
  [[nodiscard]] uint32_t size() const;
  [[nodiscard]] bool cover(const SSTable &st) const;
  [[nodiscard]] std::unique_ptr<std::string> get(const SSTableNode &n) const {
    std::ifstream fin;
    fin.open(filepath);
    bio::bitstream bin;
    fin.seekg(n.offset);
    bin.rdnbyte(fin, n.value_len);
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
class SSTableNodePool {
private:
  std::vector<std::pair<uint64_t, std::string>> _data;
  Filter_t _filter;
  uint32_t data_size;
  const uint32_t dump;
  uint64_t _id;
  enum { key_size_max = 8 };

public:
  explicit SSTableNodePool(uint64_t i, uint32_t dump_limit = 2 * 1024 * 1024);

  SSTableNodePool(const SSTableNodePool &) = delete;
  SSTableNodePool &operator=(const SSTableNodePool &) = delete;
  SSTableNodePool(SSTableNodePool &&) = delete;
  SSTableNodePool &operator=(SSTableNodePool &&) = delete;

  [[nodiscard]] Filter_t filter() const;
  [[nodiscard]] uint64_t id() const;

  bool insert(const uint64_t &key, const std::string &val);
  [[nodiscard]] SSTable write(const std::string &filepath) const;
};

SSTableNodePool::SSTableNodePool(uint64_t id, uint32_t dump_limit)
    : _data(), _filter(10240 / 2, key_size_max), data_size(0), dump(dump_limit),
      _id(id) {
  data_size += 32; // header size
}
bool SSTableNodePool::insert(const uint64_t &key, const std::string &val) {

  bio::bitstream bNew;
  bNew << val << key;
  data_size += bNew.size();

  if (data_size > dump) {
    return false;
  } else {
    _data.emplace_back(key, val);
    _filter.insert(key);
    return true;
  }
}

Filter_t SSTableNodePool::filter() const { return _filter; }

SSTable SSTableNodePool::write(const std::string &filepath) const {
  return {id(), _data, filter(), filepath};
}

[[maybe_unused]] uint64_t SSTableNodePool::id() const { return _id; }

SSTableNode::SSTableNode(const uint64_t &k, const uint32_t &o,
                         const uint32_t &l)
    : key(k), offset(o), value_len(l) {}

SSTableNode::SSTableNode() = default;

uint64_t SSTable::id() const { return _id; }

bool SSTable::check(const uint64_t &k) const {
  return filter.check(k); // Todo Use Binary Search Instead;
}

uint64_t SSTable::front() const { return table.front().key; }

uint64_t SSTable::back() const { return table.back().key; }

SSTableNode SSTable::operator[](uint32_t i) { return table[i]; }

uint32_t SSTable::size() const { return table.size(); }

bool SSTable::cover(const SSTable &st) const {
  return !(front() > st.back() || back() < st.front());
}
} // namespace kvs
#endif