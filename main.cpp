#include "lsm/lsm.h"
#include "lsm/memory_table/memory_table.h"
#include "lsm/sstable/sstable.h"
#include "lsm/sstable/sstable_manager.h"
#include "third_party/utils.h"
#include "utils.h"
#include <string>
#include "chrono"

using namespace std;

int main() {
  Lsm lsm("data");
  uint32_t size = 1000;
  for (int i = 0; i < size; ++i) {
    clock_t start = clock();
    lsm.put(i, std::string(200000, '1'));
    clock_t end = clock();
    printf("%lf\n", (double) (end - start)* 1000 / CLOCKS_PER_SEC );
  }
  return 0;
}