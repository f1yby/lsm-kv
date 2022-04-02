#ifndef KV_STORE_KVSTORE_HPP
#define KV_STORE_KVSTORE_HPP
#include "bloom_filter/bloom_filter.hpp"
#include "mem_table/mem_table.hpp"
#include "skip_list/skip_list.hpp"
#include "ss_table/ss_table.hpp"
#include "ss_table/sst_mgr.hpp"
#include <cstddef>
#include <list>
#include <string>
#include <utility>
namespace kvs {

template <typename KeyType, typename ValType> class KeyValStore {
private:
  const std::string dir;
  MemTable<KeyType, ValType> *mem_table;
  SSTMgr<KeyType, ValType> sst_mgr;

public:
  /**
   * @brief Construct a new KVStore object
   *
   * @param dir Directory to MemTable Root
   */
  explicit KeyValStore(std::string dir)
      : dir(std::move(dir)), mem_table(new MemTable<KeyType, ValType>(0)) {}
  KeyValStore() = delete;
  KeyValStore(const KeyValStore &) = delete;
  KeyValStore &operator=(const KeyValStore &) = delete;
  KeyValStore &operator=(KeyValStore &&) = delete;
  KeyValStore(KeyValStore &&) = delete;
  ~KeyValStore();

  /**
   * @brief insert/Update the key-value pair
   *
   * @param key
   * @param val
   */
  void put(const KeyType &key, const ValType &val);

  /**
   * @brief Returns the value of the given key.
   *
   * @param key
   * @return ValType
   */
  std::unique_ptr<ValType> get(const KeyType &key) const;

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
  void scan(const KeyType &key1, const KeyType &key2,
            std::list<std::pair<KeyType, ValType>> &list) const;
  void delmem(const KeyType &key) { mem_table->remove(key); }

  void dump(const std::string &filepath);
};
template <typename KeyType, typename ValType>
void KeyValStore<KeyType, ValType>::put(const KeyType &key,
                                        const ValType &val) {
  if (!mem_table->insert(key, val)) {
    sst_mgr.insert(mem_table->write(dir));
    dump(dir);
  }
}
template <typename KeyType, typename ValType>
void KeyValStore<KeyType, ValType>::scan(
    const KeyType &key1, const KeyType &key2,
    std::list<std::pair<KeyType, ValType>> &list) const {
  std::list<std::pair<KeyType, ValType>> ml = mem_table->scan(key1, key2);
  // sst_mgr.scan(ml);
  list = ml;
}
template <typename KeyType, typename ValType>
KeyValStore<KeyType, ValType>::~KeyValStore() {
  delete mem_table;
}
template <typename KeyType, typename ValType>
void KeyValStore<KeyType, ValType>::reset() {
  delete mem_table;
  mem_table = new MemTable<KeyType, ValType>(0);
  sst_mgr.clear();
}
template <typename KeyType, typename ValType>
std::unique_ptr<ValType>
KeyValStore<KeyType, ValType>::get(const KeyType &key) const {
  auto p_ans = mem_table->search(key);

  if (p_ans != nullptr) {
    return std::unique_ptr<ValType>(new ValType(*p_ans));
  } else {
    return sst_mgr.search(key);
  }
}
template <typename KeyType, typename ValType>
void KeyValStore<KeyType, ValType>::dump(const std::string &filepath) {
  sst_mgr.insert(mem_table->write(std::string().append(filepath)));
  auto new_table = new MemTable<KeyType, ValType>(mem_table->id() + 1);
  delete mem_table;
  mem_table = new_table;
}
} // namespace kvs
#endif