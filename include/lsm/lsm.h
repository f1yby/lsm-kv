#pragma once

#include "kvstore_api.h"

#include "memory_table/memory_table.h"
#include "sstable/sstable_manager.h"
#include <cstdint>
#include <string>
class Lsm : public KVStoreAPI {
private:
  const std::string dir;
  kvs::MemTable *mTable;
  kvs::SSTMgr sst_mgr;

public:
  explicit Lsm(const std::string &dir);
  Lsm(const Lsm &) = delete;
  Lsm(const Lsm &&) = delete;
  Lsm operator=(const Lsm &) = delete;
  Lsm &operator=(Lsm &&) = delete;
  void put(uint64_t key, const std::string &s) override;
  ~Lsm();

  std::string get(uint64_t key) override;

  bool del(uint64_t key) override;

  void reset() override;

  void scan(uint64_t key1, uint64_t key2,
            std::list<std::pair<uint64_t, std::string>> &list) override;

  void dump(const std::string &filepath);
};
