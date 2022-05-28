#ifndef BLOOM_FILTER_BLOOM_FILTER_HPP
#define BLOOM_FILTER_BLOOM_FILTER_HPP

#include "./hash.hpp"
#include "MurmurHash3.h"
#include "utils/bitstream.h"
#include <functional>
#include <iostream>
#include <map>
#include <vector>

template<typename T, typename H = MurMurHash<T>>
class BloomFilter {
private:
  H *_hash;
  std::vector<std::uint16_t> _data;
  std::size_t _m{};

public:
  explicit BloomFilter(std::size_t m, std::size_t s = 0);

  BloomFilter(const BloomFilter &b);

  BloomFilter();

  ~BloomFilter();

  [[nodiscard]] std::vector<std::uint16_t> data() const;

  BloomFilter &operator=(const BloomFilter &b) {
    if (&b == this) {
      return *this;
    }

    this->_hash = new H(*b._hash);
    this->_data = b._data;
    this->_m = b._m;
    return *this;
  }

  BloomFilter &operator=(BloomFilter &&b) noexcept {
    _hash = b._hash;
    b._hash = nullptr;
    _data = std::move(b.data());
    _m = b._m;
    return *this;
  }

  BloomFilter(BloomFilter &&b) noexcept
      : _hash(b._hash), _data(std::move(b._data)), _m(b._m) {
    b._hash = nullptr;
  }

  void insert(const T &key);

  bool check(const T &key) const;

  void setData(const std::vector<std::uint16_t> &data);

  template<typename FT, typename FH>
  friend bio::bitstream &operator<<(bio::bitstream &b,
                                    const BloomFilter<FT, FH> &f);
};

template<typename FT, typename FH>
bio::bitstream &operator<<(bio::bitstream &b, const BloomFilter<FT, FH> &f) {
  for (auto i: f._data) {
    b << i;
  }
  return b;
}

template<typename T, typename H>
BloomFilter<T, H>::BloomFilter(const BloomFilter &b)
    : _hash(new H(*b._hash)), _data(b._data), _m(b._m) {}

template<typename T, typename H>
BloomFilter<T, H>::~BloomFilter() {
  delete _hash;
}

template<typename T, typename H>
void BloomFilter<T, H>::insert(const T &key) {
  auto v = (*_hash)(key);
  for (auto i: *v) {
    _data[(i >> 4) % _m] |= 1 << (i & 0xF);
  }
}

template<typename T, typename H>
bool BloomFilter<T, H>::check(const T &key) const {
  auto v = (*_hash)(key);
  for (auto i: *v) {
    if ((_data[(i >> 4) % _m] & (1 << (i & 0xF))) == 0) {
      return false;
    }
  }
  return true;
}

template<typename T, typename H>
BloomFilter<T, H>::BloomFilter(std::size_t m, std::size_t s)
    : _hash(new H(s)), _data(std::vector<std::uint16_t>(m, 0)), _m(m) {
}

template<typename T, typename H>
std::vector<std::uint16_t> BloomFilter<T, H>::data() const {
  return _data;
}

template<typename T, typename H>
void BloomFilter<T, H>::setData(const std::vector<std::uint16_t> &data) {
  _data = data;
}

template<typename T, typename H>
BloomFilter<T, H>::BloomFilter() : _data({}), _hash(nullptr){};

#endif