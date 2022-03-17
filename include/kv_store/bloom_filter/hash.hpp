#ifndef KV_STORE_BLOOM_FILTER_HASH_HPP
#define KV_STORE_BLOOM_FILTER_HASH_HPP
#include "MurmurHash3.h"
#include <cstdint>
#include <functional>
#include <random>
template <typename T> class Hash {
private:
  const std::size_t _round;
  std::vector<std::size_t> _rounds;
  std::vector<std::size_t> _pool;

public:
  explicit Hash<T>(std::size_t r) : _round(r) {
    _rounds.resize(_round, 0);
    _pool.resize(_round, 0);
    std::random_device dev;
    std::mt19937 rng(dev());
    std::uniform_int_distribution<std::mt19937::result_type> dist;
    for (auto &i : _rounds) {
      i = dist(rng);
    }
  }
  std::vector<std::size_t> *operator()(const T &key) {
    std::size_t ret = std::hash<T>{}(key);
    for (int i = 0; i < _round; ++i) {
      ret ^= _rounds[i];
      _pool[i] = ret;
    }
    return &_pool;
  }
};
template <typename T> class MurMurHash {
private:
  const std::size_t _round;
  std::vector<std::size_t> _rounds;
  std::vector<std::size_t> _pool;

public:
  explicit MurMurHash<T>(std::size_t r) : _round(r > 4 ? 4 : r) {
    _rounds.resize(_round, 0);
    _pool.resize(_round, 0);
  }
  std::vector<std::size_t> *operator()(const T &key) {
    std::uint32_t out[4];
    MurmurHash3_x64_128(&key, sizeof(key), 0, out);
    for (int i = 0; i < _round; ++i) {
      _pool[i] = out[i];
    }
    return &_pool;
  }
};
#endif