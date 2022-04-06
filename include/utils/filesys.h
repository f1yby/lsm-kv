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
  utils::mkdir(dir.c_str());
  std::vector<std::string> ret;
  utils::scanDir(dir, ret);
  int i = 0;
  std::string fn = filename + "." + sufix;
  auto ans = std::find(ret.begin(), ret.end(), fn);
  while (ans != ret.end()) {
    ++i;
    fn = std::string()
             .append(filename)
             .append("(")
             .append(std::to_string(i))
             .append(")")
             .append(".")
             .append(sufix);
    ans = std::find(ret.begin(), ret.end(), fn);
  }
  return dir + "/" + fn;
}
#endif