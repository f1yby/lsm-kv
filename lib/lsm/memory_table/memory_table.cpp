#include "lsm/memory_table/memory_table.h"
namespace kvs {
MemTable::MemTable(uint64_t id, uint32_t dump_limit)
    : _skip_list(2), _filter(10240 / 2, key_size_max), data_size(0),
      dump(dump_limit), _key_min(nullptr), _key_max(nullptr), _id(id) {
  data_size += 32; // header size
  data_size += 10240;
}
bool MemTable::insert(const uint64_t &key, const std::string &val) {
  std::string *v = _skip_list.search(key);
  if (v) {
    data_size += val.size();
    data_size -= v->size();
  } else {
    data_size += key_size_max + sizeof(uint32_t) + val.size();
  }
  if (data_size > dump) {
    return false;
  }
  if (_key_min == nullptr || key < *_key_min) {
    delete _key_min;
    _key_min = new uint64_t(key);
  }
  if (_key_max == nullptr || *_key_max < key) {
    delete _key_max;
    _key_max = new uint64_t(key);
  }

  _skip_list.insert(key, val);
  _filter.insert(key);
  return true;
}

std::string *MemTable::search(const uint64_t &key) const {
  if (_filter.check(key)) {
    return _skip_list.search(key);
  }
  return nullptr;
}

Filter_t MemTable::filter() const { return _filter; }

const uint64_t *MemTable::key_min() const { return _key_min; }

const uint64_t *MemTable::key_max() const { return _key_max; }

SSTable MemTable::write(const std::string &filepath) const {
  std::vector<std::pair<uint64_t, std::string>> v(_skip_list.size());
  int j = 0;
  for (auto i = _skip_list.begin(); i != _skip_list.end();
       i = i->forwards[0], ++j) {
    v[j].first = i->key;
    v[j].second = i->val;
  }
  return {id(), v, filter(), filepath};
}

MemTable::~MemTable() {
  delete _key_min;
  delete _key_max;
}

uint64_t MemTable::size() const { return _skip_list.size(); }
std::list<std::pair<uint64_t, std::string>>
MemTable::scan(const uint64_t &start, const uint64_t &end) const {
  std::list<std::pair<uint64_t, std::string>> ans;
  for (auto i : _skip_list.scan(start, end)) {
    ans.emplace_back(i->key, i->val);
  }
  return ans;
}
[[maybe_unused]] uint64_t MemTable::id() const { return _id; }
void MemTable::remove(const uint64_t &key) {
  std::string *val = _skip_list.search(key);
  if (val != nullptr) {
    data_size -= key_size_max + sizeof(uint32_t) + val->size();
  }
  _skip_list.remove(key);
}
} // namespace kvs