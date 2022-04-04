#ifndef UTILS_FILESYS_H
#define UTILS_FILESYS_H
#include "utils.h"
#include <algorithm>
#include <functional>
#include <string>
#include <vector>

inline void RecursiveRMDir(const std::string &dir) {
  std::vector<std::string> points;
  utils::scanDir(dir, points);
  for (const auto &i : points) {
    auto fp = std::string().append(dir).append("/").append(i);
    if (utils::rmfile(fp.c_str())) {
      RecursiveRMDir(fp);
    }
  }
  utils::rmdir(dir.c_str());
}

inline std::string touch(const std::string &dir, const std::string &filename,
                         const std::string &sufix) {
  std::vector<std::string> ret;
  utils::scanDir(dir, ret);
  int i = 0;
  std::string fn = filename + "." + sufix;
  auto ans = std::find(ret.begin(), ret.end(), fn);
  while (ans != ret.end()) {
    ++i;
    fn = filename + "(" + std::to_string(i) + ")" + "." + sufix;
    ans = std::find(ret.begin(), ret.end(), fn);
  }
  return fn;
}
#endif