#ifndef KV_STORE_KVSTORE_HPP
#define KV_STORE_KVSTORE_HPP
#include "./bloom_filter/bloom_filter.hpp"
#include "./skip_list/skip_list.hpp"
#include "./ss_table/ss_table.hpp"
#include "./ss_table/index.hpp"
#include <cstddef>
#include <list>
#include <string>
#include <utility>
template <typename KeyType, typename ValType> class KeyValStore {
private:
  /**
   * @brief Directory to SSTable Root
   *
   */
  const std::string dir;
  SSTable<KeyType, ValType> *sstable;

public:
  /**
   * @brief Construct a new KVStore object
   *
   * @param dir Directory to SSTable Root
   */
  explicit KeyValStore(std::string dir, std::size_t m = 10240)
      : dir(std::move(dir)),
        sstable(new SSTable<KeyType, ValType>("data/lvl0")) {}
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
   * @brief Delete the given key-value pair if it exists.
   *
   * @param key
   * @return true key found
   * @return false key not found
   */

  bool del(const KeyType &key);

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
#endif