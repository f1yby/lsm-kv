#include "lsm/lsm.h"
#include "lsm/mem_table/mem_table.hpp"
#include "lsm/ss_table/ss_table.h"
#include "lsm/ss_table/sst_mgr.h"
#include "third_party/utils.h"
#include "utils.h"
#include <string>
using namespace std;

int main() {
  kvs::MemTable table;
  table.insert(1, "hello");
  table.insert(2, "world");
  std::cout << *table.search(1) << std::endl;
  for (const auto &i : table.scan(0, 3)) {
    std::cout << i.second << std::endl;
  }
  kvs::SSTMgr ssm;
  ssm.search(10);
  std::cout << table.write("1.txt").check(2) << std::endl;

  // KVS
  kvs::Lsm kvst(".");

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