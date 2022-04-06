#ifndef KV_STORE_SS_TABLE_INDEX
#define KV_STORE_SS_TABLE_INDEX
#include "../../bloom_filter/bloom_filter.hpp"
#include <memory>
using Filter_t = BloomFilter<uint64_t>;
namespace kvs {

class SSTableNode {
public:
  uint64_t key = 0;
  uint32_t offset = 0;
  uint32_t value_len = 0;
  SSTableNode(const uint64_t &k, const uint32_t &o, const uint32_t &l);
  SSTableNode();
};

class SSTable {
private:
  uint64_t _id;
  std::vector<SSTableNode> table;
  BloomFilter<uint64_t> filter;
  enum { key_size_max = 8 };

public:
  std::string filepath;
  SSTable(uint64_t id,
          const std::vector<std::pair<uint64_t, std::string>> &data,
          const BloomFilter<uint64_t> &filter, std::string fp);

  [[nodiscard]] uint64_t id() const;
  [[nodiscard]] bool check(const uint64_t &k) const;
  [[nodiscard]] uint64_t front() const;
  [[nodiscard]] uint64_t back() const;
  [[nodiscard]] uint32_t size() const;
  [[nodiscard]] bool cover(const SSTable &st) const;
  [[nodiscard]] std::unique_ptr<std::string> get(const SSTableNode &n) const;
  [[nodiscard]] std::unique_ptr<std::string> search(const uint64_t &key) const;
  SSTableNode operator[](uint32_t i);
  void scan(const uint64_t &key1, const uint64_t &key2,
            std::list<std::pair<uint64_t, std::string>> &list) const;
  void set_id(uint64_t i);
};

// class SSTableNodePool {
// private:
//   std::vector<std::pair<uint64_t, std::string>> _data;
//   Filter_t _filter;
//   uint32_t data_size;
//   const uint32_t dump;
//   uint64_t _id;
//   enum { key_size_max = 8 };

// public:
//   explicit SSTableNodePool(uint64_t i, uint32_t dump_limit = 2 * 1024 *
//   1024);

//   SSTableNodePool(const SSTableNodePool &) = delete;
//   SSTableNodePool &operator=(const SSTableNodePool &) = delete;
//   SSTableNodePool(SSTableNodePool &&) = delete;
//   SSTableNodePool &operator=(SSTableNodePool &&) = delete;

//   [[nodiscard]] Filter_t filter() const;
//   [[nodiscard]] uint64_t id() const;

//   bool insert(const uint64_t &key, const std::string &val);
//   [[nodiscard]] SSTable write(const std::string &filepath) const;
// };

} // namespace kvs
#endif