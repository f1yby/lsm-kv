#ifndef KV_STORE_KVSTORE_HPP
#define KV_STORE_KVSTORE_HPP
#include "./bloom_filter/bloom_filter.hpp"
#include "./skip_list/skip_list.hpp"
#include "./ss_table/ss_table.hpp"
#include <cstddef>
#include <list>
#include <string>
template <typename KeyType, typename ValType> class KeyValStore {
private:
  /**
   * @brief Directory to SSTable Root
   *
   */
  const std::string dir;
  SkipList<KeyType, ValType> memtable;
  BloomFilter<ValType> filter;
  SSTable<KeyType, ValType> sstable;

public:
  /**
   * @brief Construct a new KVStore object
   *
   * @param dir Directory to SSTable Root
   */
  KeyValStore(const std::string &dir, std::size_t m = 10240)
      : dir(dir), sstable(dir), memtable(), filter(m) {}
  KeyValStore() = delete;

  /**
   * @brief Insert/Update the key-value pair
   *
   * @param key
   * @param val
   */
  void put(KeyType key, const ValType &val);

  /**
   * @brief Returns the value of the given key.
   *
   * @param key
   * @return ValType
   */
  ValType get(KeyType key);

  /**
   * @brief Delete the given key-value pair if it exists.
   *
   * @param key
   * @return true key found
   * @return false key not found
   */

  bool del(KeyType key);

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
            std::list<std::pair<KeyType, ValType>> &list);
};
#endif