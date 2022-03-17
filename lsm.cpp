#include "kv_store/kvstore.hpp"
#include"kv_store/ss_table/ss_table.hpp"
#include <string>
using namespace std;

int main() {
  SSTable<uint64_t ,string> table("1.txt");
  table.insert(114,"hello");
  table.insert(514,"world");
  std::cout<<*table.search(114)<<std::endl;
  table.write();
  return 0;
}