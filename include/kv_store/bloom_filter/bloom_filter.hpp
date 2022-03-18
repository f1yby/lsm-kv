#ifndef BLOOM_FILTER_BLOOM_FILTER_HPP
#define BLOOM_FILTER_BLOOM_FILTER_HPP
#include "../bitstream.hpp"
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
  explicit BloomFilter(std::size_t m, std::size_t s = 0)
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
  template <typename FT, typename FH>
  friend bio::bitstream &operator<<(bio::bitstream &b,
                                    const BloomFilter<FT, FH> &f);
};
template <typename FT, typename FH>
bio::bitstream &operator<<(bio::bitstream &b, const BloomFilter<FT, FH> &f) {
  for (auto i : f._data) {
    b << i;
  }
  return b;
}

#endif