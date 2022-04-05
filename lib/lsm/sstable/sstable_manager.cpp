#include "lsm/sstable/sstable_manager.h"
#include "lsm/memory_table/memory_table.h"
#include "utils/filesys.h"
#include <cstdint>
namespace kvs {
SSTMgr::SSTMgr() : _data(1) {}

void SSTMgr::insert(const SSTable &sst) {
  _data.front().push_front(sst);
  merge();
}

std::unique_ptr<std::string> SSTMgr::search(const uint64_t &k) const {
  for (const auto &j : _data) {
    for (const auto &i : j) {
      std::unique_ptr<std::string> ans = i.search(k);
      if (ans != nullptr) {
        return ans;
      }
    }
  }
  return {nullptr};
}

void SSTMgr::clear() {
  _data.clear();
  RecursiveRMDir(_dir);
}

SSTMgr::SSTMgr(std::string dir) : _data(1), _dir(std::move(dir)) {}

void SSTMgr::scan(const uint64_t &key1, const uint64_t &key2,
                  std::list<std::pair<uint64_t, std::string>> &list) const {
  // TODO Binary Scan
  for (const auto &i : _data) {
    for (const auto &j : i) {
      j.scan(key1, key2, list);
    }
  }
}

void SSTMgr::merge() {
  if (_data.front().size() > 2) {
    if (_data.size() == 1) {
      _data.emplace_back();
    }
    auto iter = _data.begin();
    merge1(_data.front(), *(++iter));
    _data.front().clear();
    uint32_t i = 1;
    while (iter->size() > i * 2 + 2) {
      if (_data.size() == i + 1) {
        _data.emplace_back();
      }
      std::list<SSTable> v;
      auto iter_delete = iter->end();
      size_t e = iter->size() - i * 2 - 2;
      for (size_t j = 0; j < e; ++j) {
        --iter_delete;
        v.emplace_back(std::move(*iter_delete));
      }
      iter->erase(iter_delete, iter->end());
      mergeN(v, *(++iter), i + 1);
      ++i;
    }
  }
}
void SSTMgr::merge1(const std::list<SSTable> &l1, std::list<SSTable> &l2) {
  bool isFinal = l2.empty();
  uint64_t id = l1.front().id();
  for (const auto &i : l1) {
    if (i.id() > id) {
      id = i.id();
    }
  }
  std::list<SSTable> pool(l1);
  for (auto i = l2.begin(); i != l2.end();) {
    bool find = false;
    for (const auto &j : l1) {
      if (i->cover(j)) {
        pool.push_back(*i);
        i = l2.erase(i);
        find = true;
        break;
      }
    }
    if (!find) {
      ++i;
    }
  }

  auto *sp = new MemTable(id);
  std::vector<uint32_t> vector(pool.size(), 0);
  std::vector<SSTable> ans;
  while (true) {
    uint64_t key_min = 0;
    uint64_t id_max = 0;
    bool find = false;
    int ii = 0;
    for (auto i = pool.begin(); i != pool.end(); ++i, ++ii) {
      if (i->size() == vector[ii]) {
        continue;
      }
      if (!find) {
        key_min = (*i)[vector[ii]].key;
        id_max = i->id();
        find = true;
      } else {
        if ((*i)[vector[ii]].key < key_min) {
          key_min = (*i)[vector[ii]].key;
          id_max = i->id();
        } else if ((*i)[vector[ii]].key == key_min && id_max < i->id()) {
          id_max = i->id();
        }
      }
    }

    if (!find) {
      break;
    }

    auto val = std::string();
    ii = 0;
    for (auto i = pool.begin(); i != pool.end(); ++i, ++ii) {
      if ((*i).size() == vector[ii]) {
        continue;
      }
      if ((*i)[vector[ii]].key == key_min) {
        if (id_max == (*i).id()) {
          val = *(*i).get((*i)[vector[ii]]);
        }
        ++vector[ii];
      }
    }
    // if (isFinal && val == "~DELETED~") {
    //   continue;
    // }
    if (!sp->insert(key_min, val)) {
      l2.push_front(sp->write(
          touch(std::string().append(_dir).append("/").append("level-").append(
                    std::to_string(1)),
                std::to_string(id), "sst")));
      delete sp;
      sp = new MemTable(id);
      sp->insert(key_min, val);
    }
  }
  l2.push_front(sp->write(
      touch(std::string().append(_dir).append("/").append("level-").append(
                std::to_string(1)),
            std::to_string(id), "sst")));
  delete sp;
  for (const auto &i : pool) {
    utils::rmfile(i.filepath.c_str());
  }
}
void SSTMgr::mergeN(const std::list<SSTable> &l1, std::list<SSTable> &l2,
                    uint64_t lvl) {
  bool isFinal = l2.empty();
  uint64_t id = l1.front().id();
  for (const auto &i : l1) {
    if (i.id() > id) {
      id = i.id();
    }
  }
  std::list<SSTable> pool(l1);
  for (auto i = l2.begin(); i != l2.end();) {
    bool find = false;
    for (const auto &j : l1) {
      if (i->cover(j)) {
        pool.push_back(*i);
        i = l2.erase(i);
        find = true;
        break;
      }
    }
    if (!find) {
      ++i;
    }
  }

  auto *sp = new MemTable(id);
  std::vector<uint32_t> vector(pool.size(), 0);
  std::vector<SSTable> ans;
  while (true) {
    uint64_t key_min = 0;
    uint64_t id_max = 0;
    bool find = false;
    int ii = 0;
    for (auto i = pool.begin(); i != pool.end(); ++i, ++ii) {
      if (i->size() == vector[ii]) {
        continue;
      }
      if (!find) {
        key_min = (*i)[vector[ii]].key;
        id_max = i->id();
        find = true;
      } else {
        if ((*i)[vector[ii]].key < key_min) {
          key_min = (*i)[vector[ii]].key;
          id_max = i->id();
        } else if ((*i)[vector[ii]].key == key_min && id_max < i->id()) {
          id_max = i->id();
        }
      }
    }

    if (!find) {
      break;
    }

    auto val = std::string();
    ii = 0;
    for (auto i = pool.begin(); i != pool.end(); ++i, ++ii) {
      if ((*i).size() == vector[ii]) {
        continue;
      }
      if ((*i)[vector[ii]].key == key_min) {
        if (id_max == (*i).id()) {
          val = *(*i).get((*i)[vector[ii]]);
        }
        ++vector[ii];
      }
    }
    // if (isFinal && val == "~DELETED~") {
    //   continue;
    // }
    if (!sp->insert(key_min, val)) {
      l2.push_front(sp->write(
          touch(std::string().append(_dir).append("/").append("level-").append(
                    std::to_string(lvl)),
                std::to_string(id), "sst")));
      delete sp;
      sp = new MemTable(id);
      sp->insert(key_min, val);
    }
  }
  l2.push_front(sp->write(
      touch(std::string().append(_dir).append("/").append("level-").append(
                std::to_string(lvl)),
            std::to_string(id), "sst")));
  delete sp;
  for (const auto &i : pool) {
    utils::rmfile(i.filepath.c_str());
  }
}
} // namespace kvs