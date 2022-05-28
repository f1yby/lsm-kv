#include "lsm/sstable/sstable_manager.h"
#include "lsm/memory_table/memory_table.h"
#include "utils/filesys.h"
#include <cstdint>
#include <cstdlib>

namespace kvs {
  SSTMgr::SSTMgr() : _data(1) {}

  void SSTMgr::insert(const SSTable &sst) {
    _data.front().push_back(sst);
    merge();
  }

  std::unique_ptr<std::string> SSTMgr::search(const uint64_t &k) const {
    for (const auto &j: _data) {
      for (auto i = j.rbegin(); i != j.rend(); ++i) {
        std::unique_ptr<std::string> ans = i->search(k);
        if (ans != nullptr) {
          return ans;
        }
      }
    }
    return nullptr;
  }

  void SSTMgr::clear() {
    _data.clear();
    _data.emplace_back();
    RecursiveRMDir(_dir);
  }

  SSTMgr::SSTMgr(std::string dir) : _data(1), _dir(std::move(dir)) {
    if (utils::dirExists(_dir)) {
      auto iter = _data.begin();
      for (uint32_t lvl = 0;
           utils::dirExists(_dir + "/level-" + std::to_string(lvl)); ++lvl) {
        std::vector<std::string> scan_ret;
        utils::scanDir(_dir + "/level-" + std::to_string(lvl), scan_ret);
        for (const auto &i: scan_ret) {
          std::string path = _dir + "/level-" + std::to_string(lvl) + "/" + i;
          if (!utils::dirExists(path)) {
            iter->emplace_back(path);
          }
        }
      }
    } else {
      utils::mkdir(_dir.c_str());
    }
  }

  void SSTMgr::scan(const uint64_t &key1, const uint64_t &key2,
                    std::list<std::pair<uint64_t, std::string>> &list) const {
    int lvl = 0;
    for (const auto &i: _data) {
      if (lvl == 0) {
        for (auto j = i.rbegin(); j != i.rend(); ++j) {
          j->scan(key1, key2, list);
        }
      } else {
        auto beginref = i.begin();
        auto endref = i.end();
        if (!i.empty()) {
        } else if (key1 < i.front().front()) {
          endref = i.begin();
        } else if (key2 > i.back().back()) {
          beginref = i.end();
        } else {

          beginref = std::lower_bound(i.begin(), i.end(), SSTable{std::vector<SSTableNode>({{key1, 0, 0}})}, [](const SSTable &left, const SSTable &right) -> bool { return left.front() < right.front(); });

          if (beginref == i.end() || beginref->front() > key1 && beginref != i.begin()) {
            --beginref;
          }


          endref = std::upper_bound(i.begin(), i.end(), SSTable{std::vector<SSTableNode>({{key2, 0, 0}})}, [](const SSTable &left, const SSTable &right) -> bool { return left.back() < right.back(); });


          if (beginref->back() < key1) {
            ++beginref;
          }
          if (endref != i.end() && endref->front() <= key2) {
            ++endref;
          }
        }

        for (auto j = beginref; j != endref; ++j) {
          j->scan(key1, key2, list);
        }
      }
      ++lvl;
    }
  }

  void SSTMgr::merge() {
    if (_data.front().size() > 2) {
      if (_data.size() == 1) {
        _data.emplace_back();
      }
      auto iter = _data.begin();
      mergeN(_data.front(), *(++iter), 1);
      _data.front().clear();
      uint32_t i = 1;
      uint32_t max = 4;
      while (iter->size() > max) {
        if (_data.size() == i + 1) {
          _data.emplace_back();
        }
        std::vector<SSTable> v;

        size_t e = iter->size() - i * 2 - 2;
        for (size_t j = 0; j < e; ++j) {
          auto iter_delete = iter->begin();
          auto id = iter->begin()->id();
          for (auto ii = iter->begin(); ii != iter->end(); ++ii) {
            if (ii->id() < id) {
              id = ii->id();
              iter_delete = ii;
            }
          }
          v.emplace_back(std::move(*iter_delete));
          iter->erase(iter_delete);
        }

        mergeN(v, *(++iter), i + 1);
        ++i;
        max *= 2;
      }
    }
  }


  void SSTMgr::mergeN(const std::vector<SSTable> &l1, std::vector<SSTable> &l2, uint64_t lvl) {
    bool isFinal = l2.empty();
    uint64_t id = l1.front().id();
    uint64_t min = l1.front().front();
    uint64_t max = l1.back().back();
    std::list<std::pair<uint64_t, std::string>> list;

    for (const auto &i: l1) {
      i.scan(i.front(), i.back(), list);
      utils::rmfile(i.filepath.c_str());
    }
    for (const auto &i: l1) {
      if (i.id() > id) {
        id = i.id();
      }
      if (i.front() < min) {
        min = i.front();
      }
      if (i.back() > max) {
        max = i.back();
      }
    }

    auto beginref = l2.begin();
    auto endref = l2.end();
    if (l2.empty()) {
    } else if (max < l2.front().front()) {
      endref = l2.begin();
    } else if (min > l2.back().back()) {
      beginref = l2.end();
    } else {

      beginref = std::lower_bound(l2.begin(), l2.end(), SSTable{std::vector<SSTableNode>({{min, 0, 0}})}, [](const SSTable &left, const SSTable &right) -> bool { return left.front() < right.front(); });

      if (beginref == l2.end() || beginref->front() > min && beginref != l2.begin()) {
        --beginref;
      }


      endref = std::upper_bound(l2.begin(), l2.end(), SSTable{std::vector<SSTableNode>({{max, 0, 0}})}, [](const SSTable &left, const SSTable &right) -> bool { return left.back() < right.back(); });


      if (beginref->back() < min) {
        ++beginref;
      }
      if (endref != l2.end() && endref->front() <= max) {
        ++endref;
      }
    }


    for (auto i = beginref; i != endref; ++i) {
      i->scan(i->front(), i->back(), list);
      utils::rmfile(i->filepath.c_str());
    }
    auto insert_point = endref;
    if (beginref != endref) {
      insert_point = l2.erase(beginref, endref);
    }
    std::vector<SSTable> pool;

    auto *sp = new SSTableNodePool(id);
    for (const auto &i: list) {
      if (isFinal && i.second == "~DELETED~") {
        continue;
      }
      if (!sp->insert(i.first, i.second)) {
        pool.push_back(sp->write(
                touch(std::string().append(_dir).append("/").append("level-").append(
                              std::to_string(lvl)),
                      std::to_string(id), "sst")));
        delete sp;
        sp = new SSTableNodePool(id);
        sp->insert(i.first, i.second);
      }
    }


    pool.push_back(sp->write(
            touch(std::string().append(_dir).append("/").append("level-").append(
                          std::to_string(lvl)),
                  std::to_string(id), "sst")));
    l2.insert(insert_point, std::make_move_iterator(pool.begin()), std::make_move_iterator(pool.end()));

    delete sp;
  }
}// namespace kvs