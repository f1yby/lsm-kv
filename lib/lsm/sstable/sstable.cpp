#include "lsm/sstable/sstable.h"
#include "cstring"
#include <fstream>
namespace kvs {
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

uint64_t SSTableNodePool::id() const { return _id; }

SSTableNode::SSTableNode(const uint64_t &k, const uint32_t &o,
                         const uint32_t &l)
    : key(k), offset(o), value_len(l) {}

SSTableNode::SSTableNode() = default;

uint64_t SSTable::id() const { return _id; }

bool SSTable::check(const uint64_t &k) const {
  return filter.check(k);
}

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

  static std::array<uint8_t, 2 * 1024 * 1024> pool;

  auto fd = fopen(filepath.c_str(), "w");
  auto size = data.size();

  memcpy(&pool[0], &id, sizeof(id));
  memcpy(&pool[8], &size, sizeof(size));
  memcpy(&pool[16], &data.front().first, sizeof(data.front().first));
  memcpy(&pool[24], &data.back().first, sizeof(data.back().first));

  uint32_t KOffset = 32;
  memcpy(&pool[KOffset], filter.data().data(), filter.data().size());
  KOffset += 10240;
  uint32_t VOffset = KOffset + (key_size_max + sizeof(VOffset)) * data.size();

  table.resize(data.size());
  for (uint32_t i = 0; i < table.size(); ++i) {
    memcpy(&pool[KOffset], &data[i].first, sizeof(data[i].first));

    KOffset += key_size_max;
    memcpy(&pool[KOffset], &VOffset, sizeof(VOffset));
    KOffset += sizeof(VOffset);
    table[i].key = data[i].first;
    table[i].value_len = data[i].second.size();
    table[i].offset = VOffset;

    memcpy(&pool[VOffset], data[i].second.c_str(), data[i].second.size());

    VOffset += data[i].second.size();
  }
  fwrite(pool.data(), pool.size(), 1, fd);
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
void SSTable::scan(const uint64_t &key1, const uint64_t &key2,
                   std::list<std::pair<uint64_t, std::string>> &list) const {
//  static std::array<uint8_t,2*1024*1024>pool;
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
} // namespace kvs