#include "lsm/sstable/sstable.h"
#include "cstring"
#include <algorithm>
#include <array>
#include <fstream>
#include <utility>

template<class Iter, class T, class Compare>
static Iter binary_find(Iter begin, Iter end, T val, Compare compare) {
  // Finds the lower bound in at most log(last - first) + 1 comparisons
  Iter i = std::lower_bound(begin, end, val, compare);

  return i;
}

namespace kvs {
  SSTableNode::SSTableNode(const uint64_t &k, const uint32_t &o,
                           const uint32_t &l)
      : key(k), offset(o), value_len(l) {}

  SSTableNode::SSTableNode() = default;

  uint64_t SSTable::id() const { return _id; }

  bool SSTable::check(const uint64_t &k) const { return filter.check(k); }

  uint64_t SSTable::front() const { return table.front().key; }

  uint64_t SSTable::back() const { return table.back().key; }

  SSTableNode SSTable::operator[](uint32_t i) { return table[i]; }

  uint32_t SSTable::size() const { return table.size(); }

  bool SSTable::cover(const SSTable &st) const {
    return !(front() > st.back() || back() < st.front());
  }

  SSTable::SSTable(uint64_t id,
                   const std::vector<std::pair<uint64_t, std::string>> &data,
                   const BloomFilter<uint64_t> &filter, std::string fp)
      : _id(id), filter(filter), filepath(std::move(fp)) {
    std::array<uint8_t, 2 * 1024 * 1024> pool{};

    auto size = data.size();

    memcpy(&pool[0], &id, 8);
    memcpy(&pool[8], &size, 8);
    memcpy(&pool[16], &data.front().first, 8);
    memcpy(&pool[24], &data.back().first, 8);

    uint32_t KOffset = 32;
    memcpy(&pool[KOffset], filter.data().data(), 10240);
    KOffset += 10240;
    uint32_t VOffset = KOffset + (key_size_max + sizeof(VOffset)) * data.size();

    table.resize(data.size());
    for (uint32_t i = 0; i < table.size(); ++i) {

      table[i].key = data[i].first;
      memcpy(&pool[KOffset], &data[i].first, key_size_max);
      KOffset += key_size_max;

      table[i].offset = VOffset;
      memcpy(&pool[KOffset], &VOffset, sizeof(uint32_t));
      KOffset += sizeof(uint32_t);

      table[i].value_len = data[i].second.size();
      memcpy(&pool[VOffset], data[i].second.c_str(), data[i].second.size());
      VOffset += data[i].second.size();
    }
    if (VOffset > 2 * 1024 * 1024) {
      abort();
    }
    auto fd = fopen(filepath.c_str(), "w");
    fwrite(pool.data(), VOffset, 1, fd);
    fclose(fd);
  }

  std::unique_ptr<std::string> SSTable::get(const SSTableNode &n) const {
    auto fd = fopen(filepath.c_str(), "r");
    fseek(fd, n.offset, SEEK_SET);
    auto ans = std::string(n.value_len, '\0');
    if (!fread(&ans[0], n.value_len, 1, fd)) {
      abort();
    }
    fclose(fd);
    return std::make_unique<std::string>(ans);
  }

  std::unique_ptr<std::string> SSTable::search(const uint64_t &key) const {
    if ((key < front() || back() < key)) {
      return {nullptr};
    }
    SSTableNode node;
    node.key = key;
    auto iter = std::lower_bound(table.begin(), table.end(), node,
                                 [](const SSTableNode &left, const SSTableNode &right) -> bool {
                                   return left.key < right.key;
                                 });
    if (iter != table.end() && iter->key == key) {
      return get(*iter);
    }
    return nullptr;
  }

  void SSTable::scan(const uint64_t &key1, const uint64_t &key2,
                     std::list<std::pair<uint64_t, std::string>> &list) const {
    if (!(key1 > table.back().key || key2 < table.front().key)) {
      SSTableNode node;
      node.key = key1;
      auto begin = table.begin();
      if (table.begin()->key < key1) {
        begin = std::lower_bound(table.begin(), table.end(), node,
                                 [](const SSTableNode &left, const SSTableNode &right) -> bool {
                                   return left.key < right.key;
                                 });
      }


      node.key = key2;
      auto end = table.end();
      if (table.back().key > key2) {
        end = std::upper_bound(table.begin(), table.end(), node,
                               [](const SSTableNode &left, const SSTableNode &right) -> bool {
                                 return left.key < right.key;
                               });
      }

      std::pair<uint64_t, std::string> pair({key1, ""});

      auto j = list.begin();

      while (begin != end) {
        if (j == list.end() || begin->key < j->first) {
          j = list.insert(j, {begin->key, *get(*begin)});
          ++j;
          ++begin;
        } else if (begin->key == j->first) {
          ++begin;
          ++j;
        } else {
          ++j;
        }
      }
    }
  }

  void SSTable::set_id(uint64_t i) { _id = i; }

  SSTable::SSTable(const std::string &filepath)
      : filepath(filepath), filter(10240 / 2, key_size_max), _id(0) {
    static std::array<uint8_t, 2 * 1024 * 1024> pool;
    auto fd = fopen(filepath.c_str(), "r");
    uint64_t size;
    std::vector<std::uint16_t> fv(10240 / 2, 0);

    fread(&pool[0], 1, pool.size(), fd);
    uint32_t VOffsetEnd = ftell(fd);
    memcpy(&_id, &pool[0], 8);
    memcpy(&size, &pool[8], 8);
    memcpy(&fv[0], &pool[32], 10240);
    filter.setData(fv);
    uint32_t KOffset = 32;
    KOffset += 10240;
    table.resize(size);
    uint32_t VOffsetBase =
            KOffset + (key_size_max + sizeof(VOffsetBase)) * table.size();

    for (auto &i: table) {

      memcpy(&i.key, &pool[KOffset], key_size_max);
      KOffset += key_size_max;

      memcpy(&i.offset, &pool[KOffset], sizeof(uint32_t));
      KOffset += sizeof(uint32_t);
    }
    for (int i = 1; i < table.size(); ++i) {
      table[i - 1].value_len = table[i].offset - VOffsetBase;
      VOffsetBase = table[i].offset;
    }
    table.back().value_len = VOffsetEnd - table.back().offset;
    fclose(fd);
  }

  SSTable::SSTable(std::vector<SSTableNode> data) : table(std::move(data)), _id(0) {}
}// namespace kvs