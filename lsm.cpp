#include "lsm/lsm.hpp"
#include "lsm/mem_table/mem_table.hpp"
#include "lsm/ss_table/ss_table.hpp"
#include "lsm/ss_table/sst_mgr.hpp"
#include "third_party/utils.h"
#include "utils.h"
#include <string>
using namespace std;

int main() {
  kvs::MemTable<uint64_t, string> table;
  table.insert(1, "hello");
  table.insert(2, "world");
  std::cout << *table.search(1) << std::endl;
  for (const auto &i : table.scan(0, 3)) {
    std::cout << i.second << std::endl;
  }
  kvs::SSTMgr<uint64_t, std::string> ssm;
  ssm.search(10);
  std::cout << table.write("1.txt").check(2) << std::endl;

  // KVS
  kvs::Lsm<uint64_t, string> kvst(".");

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
  utils::rmfile("1.txt");
  return 0;
}