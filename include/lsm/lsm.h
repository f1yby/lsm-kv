#ifndef KV_STORE_KVSTORE_HPP
#define KV_STORE_KVSTORE_HPP
#include "bloom_filter/bloom_filter.hpp"
#include "mem_table/mem_table.hpp"
#include "skip_list/skip_list.hpp"
#include "lsm/ss_table/ss_table.h"
#include "ss_table/sst_mgr.h"
#include <cstddef>
#include <list>
#include <memory>
#include <string>
#include <utility>
namespace kvs {

class Lsm {
private:
  const std::string dir;
  MemTable *mem_table;
  SSTMgr sst_mgr;

public:
  /**
   * @brief Construct a new KVStore object
   *
   * @param dir Directory to MemTable Root
   */
  explicit Lsm(const std::string &dir) : dir(dir), mem_table(new MemTable(0)) {
    utils::mkdir(dir.c_str());
  }
  Lsm() = delete;
  Lsm(const Lsm &) = delete;
  Lsm &operator=(const Lsm &) = delete;
  Lsm &operator=(Lsm &&) = delete;
  Lsm(Lsm &&) = delete;
  ~Lsm();

  /**
   * @brief insert/Update the key-value pair
   *
   * @param key
   * @param val
   */
  void put(const uint64_t &key, const std::string &val);

  /**
   * @brief Returns the value of the given key.
   *
   * @param key
   * @return std::string   */
  [[nodiscard]] std::unique_ptr<std::string> get(const uint64_t &key) const;

  /**
   * @brief This resets the kvstore. All key-value pairs should be removed,
   * including memtable and all sstables files.
   *
   */
  void reset();

  /**
   * @brief Return a list including all the key-value pair between key1 and
   * key2.keys in the list should be in an ascending order.An empty list
   * indicates not found.
   *
   * @param key1
   * @param key2
   * @param list
   */
  void scan(const uint64_t &key1, const uint64_t &key2,
            std::list<std::pair<uint64_t, std::string>> &list) const;
  void delmem(const uint64_t &key) { mem_table->remove(key); }

  void dump(const std::string &filepath);
};

void Lsm::put(const uint64_t &key, const std::string &val) {
  if (!mem_table->insert(key, val)) {
    dump(dir + '/' + std::to_string(mem_table->id()) + ".sst");
    mem_table->insert(key, val);
  }
}

void Lsm::scan(const uint64_t &key1, const uint64_t &key2,
               std::list<std::pair<uint64_t, std::string>> &list) const {
  std::list<std::pair<uint64_t, std::string>> ml = mem_table->scan(key1, key2);
  sst_mgr.scan(key1, key2, ml);
  list = ml;
}

Lsm::~Lsm() {
  delete mem_table;
  RecursiveRMDir(dir);
}

void Lsm::reset() {
  delete mem_table;
  mem_table = new MemTable(0);
  sst_mgr.clear();
}

std::unique_ptr<std::string> Lsm::get(const uint64_t &key) const {
  auto p_ans = mem_table->search(key);

  if (p_ans != nullptr) {
    return std::make_unique<std::string>(*p_ans);
  } else {
    return sst_mgr.search(key);
  }
}

void Lsm::dump(const std::string &filepath) {
  sst_mgr.insert(mem_table->write(std::string().append(filepath)));
  auto new_table = new MemTable(mem_table->id() + 1);
  delete mem_table;
  mem_table = new_table;
}
} // namespace kvs
#endif