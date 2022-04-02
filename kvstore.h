#pragma once

#include "kvstore_api.h"
#include "lsm/lsm.hpp"
#include <cstdint>
#include <string>
class KVStore : public KVStoreAPI {
  // You can add your implementation here
private:
  kvs::KeyValStore<uint64_t, std::string> store;

public:
  KVStore(const std::string &dir) : store(dir) {}
  ~KVStore() = default;
  void put(uint64_t key, const std::string &s) override { store.put(key, s); }

  std::string get(uint64_t key) override {
    auto p = store.get(key);
    return p == nullptr ? std::string() : *p;
  }

  bool del(uint64_t key) override {
    if (store.get(key) != nullptr) {
      store.delmem(key);
      if (store.get(key) != nullptr) {
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
  }
};
