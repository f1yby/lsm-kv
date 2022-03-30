#include "kv_store/kvstore.hpp"
#include "kv_store/mem_table/mem_table.hpp"
#include "kv_store/ss_table/ss_table.hpp"
#include "kv_store/ss_table/sst_mgr.hpp"
#include "utils.h"
#include <string>
using namespace std;

int main() {
  kvs::MemTable<uint64_t, string> table;
  table.insert(1, "hello");
  table.insert(2, "world");
  std::cout << *table.search(1) << std::endl;
  for (auto i : table.scan(0, 3)) {
    std::cout << i->val << std::endl;
  }
  kvs::SSTMgr<uint64_t, std::string> ssm;
  ssm.search(10);
  std::cout << table.write("1.txt").check(2) << std::endl;

  // KVS
  kvs::KeyValStore<uint64_t, string> kvst(".");

  kvst.put(1, "Hello");
  // get from memory
  std::cout << *kvst.get(1) << std::endl;

  kvst.dump("1.txt");
  // get from disk
  std::cout << *kvst.get(1) << std::endl;

  kvst.put(1, "world");
  // get form memory
  std::cout << *kvst.get(1) << std::endl;
  // Utils
  std::vector<std::string> v;
  utils::scanDir(".", v);
  for (const auto &i : v) {
    std::cout << i << std::endl;
  }
  utils::rmfile("1.txt");
  return 0;
}