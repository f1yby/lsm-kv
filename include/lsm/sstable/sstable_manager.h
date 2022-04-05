//
// Created by jiarui on 3/18/22.
//

#ifndef LSM_KV_SST_MGR_H
#define LSM_KV_SST_MGR_H
#include "algorithm"
#include "sstable.h"
#include "utils.h"
#include "utils/filesys.h"
#include <cstdint>
#include <list>
#include <utility>
#include <vector>

namespace kvs {
class SSTMgr {
private:
  std::vector<std::vector<SSTable>> _data;
  const std::string _dir;
  void merge();
  void merge2(const std::list<SSTable> &l1, std::vector<SSTable> &l2,
              const uint64_t lvl) {
    // TODO
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
    std::vector<int> vector(pool.size(), 0);
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
      ii=0;
      for (auto i=pool.begin(); i != pool.end(); ++i,++ii) {
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
      if (!sp->insert(key_min, val)) {
        l2.push_back(sp->write(touch(
            std::string().append(_dir).append("/").append("level-").append(
                std::to_string(lvl)),
            std::to_string(id), "sst")));
        delete sp;
        sp = new MemTable(id);
      }
    }
    l2.push_back(sp->write(
        touch(std::string().append(_dir).append("/").append("level-").append(
                  std::to_string(lvl)),
              std::to_string(id), "sst")));
    delete sp;
  }

public:
  SSTMgr();
  explicit SSTMgr(std::string dir);
  void insert(const SSTable &sst);
  [[nodiscard]] std::unique_ptr<std::string> search(const uint64_t &k) const;
  void clear();
  void scan(const uint64_t &key1, const uint64_t &key2,
            std::list<std::pair<uint64_t, std::string>> &list) const;
};

SSTMgr::SSTMgr() : _data(1) {}

void SSTMgr::insert(const SSTable &sst) {
  // Todo Hierarchy
  _data[0].push_back(sst);
 //merge();
}

std::unique_ptr<std::string> SSTMgr::search(const uint64_t &k) const {
  for (const auto &j : _data) {
    for (SSTable i : j) {
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
  if (_data[0].size() > 2) {
    if (_data.size() == 1) {
      _data.emplace_back();
    }
    merge2(std::list(_data[0].begin(),_data[0].end()), _data[1], 1);
    _data[0].clear();
    int i = 1;
    while (_data[i].size() > i * 2 + 2) {
      if (_data.size() == i + 1) {
        _data.emplace_back();
      }
      std::list<SSTable> v;
      size_t e = _data[i].size() - i * 2 - 2;
      for (size_t j = 0; j < e; ++j) {
        v.push_back(_data[i][j]);
      }
      _data[i].erase(_data[i].begin(), _data[i].begin() + (int)e);
      merge2(v, _data[i + 1], i + 1);
      ++i;
    }
  }
}

} // namespace kvs
#endif // LSM_KV_SST_MGR_H
