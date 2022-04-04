#pragma once

#include "kvstore_api.h"
#include "lsm/lsm.h"
#include <cstdint>
#include <string>
class KVStore : public KVStoreAPI {
  // You can add your implementation here
private:
  kvs::Lsm store;

public:
  explicit KVStore(const std::string &dir) : KVStoreAPI(dir), store(dir) {}
  ~KVStore() = default;
  KVStore(const KVStore &) = delete;
  KVStore(const KVStore &&) = delete;
  KVStore operator=(const KVStore &) = delete;
  KVStore &operator=(KVStore &&) = delete;
  void put(uint64_t key, const std::string &s) override { store.put(key, s); }

  std::string get(uint64_t key) override {
    auto p = store.get(key);
    return p == nullptr || *p == "~DELETED~" ? std::string() : *p;
  }

  bool del(uint64_t key) override {
    auto pair = store.get(key);
    if (pair != nullptr && *pair != "~DELETED~") {
      store.delmem(key);
      pair = store.get(key);
      if (pair != nullptr && *pair != "~DELETED~") {
        store.put(key, "~DELETED~");
      }
      return true;
    }
    return false;
  }

  void reset() override { store.reset(); }

  void scan(uint64_t key1, uint64_t key2,
            std::list<std::pair<uint64_t, std::string>> &list) override {
    store.scan(key1, key2, list);
    for (auto i = list.begin(); i != list.end(); ++i) {
      if (i->second == "~DELETE~") {
        i = list.erase(i);
      }
    }
  }
};
