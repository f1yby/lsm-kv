#ifndef KV_STORE_KVSTORE_HPP
#define KV_STORE_KVSTORE_HPP
#include "kv_store/ss_table/ss_table.hpp"
#include "kv_store/bloom_filter/bloom_filter.hpp"
#include "kv_store/mem_table/mem_table.hpp"
#include "kv_store/skip_list/skip_list.hpp"
#include "kv_store/ss_table/sst_mgr.hpp"
#include <cstddef>
#include <list>
#include <string>
#include <utility>
namespace kvs {

template <typename KeyType, typename ValType> class KeyValStore {
private:
  const std::string dir;
  MemTable<KeyType, ValType> *mem_table;

public:
  /**
   * @brief Construct a new KVStore object
   *
   * @param dir Directory to MemTable Root
   */
  explicit KeyValStore(std::string dir, std::size_t m = 10240)
      : dir(std::move(dir)),
        mem_table(new MemTable<KeyType, ValType>("data/lvl0")) {}
  KeyValStore() = delete;

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
  ValType get(const KeyType &key) const;

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
  void scan(KeyType key1, KeyType key2,
            std::list<std::pair<KeyType, ValType>> &list) const;
};
template <typename KeyType, typename ValType>
void KeyValStore<KeyType, ValType>::put(const KeyType &key,
                                        const ValType &val) {}
template <typename KeyType, typename ValType>
void KeyValStore<KeyType, ValType>::scan(
    KeyType key1, KeyType key2,
    std::list<std::pair<KeyType, ValType>> &list) const {}
} // namespace kvs
#endif