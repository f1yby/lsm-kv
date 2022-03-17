#include "kv_store/kvstore.hpp"
#include <string>
using namespace std;

int main() {
  KeyValStore<int, string> kvs("./data");
  return 0;
}