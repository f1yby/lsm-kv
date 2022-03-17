#ifndef BLOOM_FILTER_BLOOM_FILTER_HPP
#define BLOOM_FILTER_BLOOM_FILTER_HPP
#include "./hash.hpp"
#include "MurmurHash3.h"
#include <functional>
#include <iostream>
#include <map>
#include <vector>

template <typename T, typename H = MurMurHash<T>> class BloomFilter {
private:
  H _hash;
  std::vector<std::uint16_t> _data;
  std::size_t _m;

public:
  BloomFilter(std::size_t m, std::size_t s = 0)
      : _data(std::vector<std::uint16_t>()), _hash(H(s)), _m(m) {
    _data.resize(m, 0);
  }
  void insert(const T &key) {
    auto v = _hash(key);
    for (auto i : *v) {
      _data[(i >> 4) % _m] |= 1 << (i & 0xF);
    }
  }
  bool check(const T &key) {
    auto v = _hash(key);
    for (auto i : *v) {
      if ((_data[(i >> 4) % _m] & (1 << (i & 0xF))) == 0) {
        return false;
      }
    }
    return true;
  }
};
#endif