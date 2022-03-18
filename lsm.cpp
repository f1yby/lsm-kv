#include "kv_store/kvstore.hpp"
#include "kv_store/mem_table/mem_table.hpp"
#include "kv_store/ss_table/ss_table.hpp"
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
  std::cout << table.write("1.txt").check(3) << std::endl;
  return 0;
}