//
// Created by jiarui on 2022/3/17.
//

#ifndef LSM_KV_BITSTREAM_HPP
#define LSM_KV_BITSTREAM_HPP
#include <iostream>
#include <list>
namespace bio {
class bitstream {
  std::list<uint8_t> buffer;

public:
  bitstream() = default;
  std::size_t size() { return buffer.size(); }
  void clear() { buffer.clear(); }
  friend std::ostream &operator<<(std::ostream &o, bitstream &b);
  friend bitstream &operator<<(bitstream &b, uint8_t uint8);
  friend bitstream &operator<<(bitstream &b, uint16_t uint16);
  friend bitstream &operator<<(bitstream &b, uint32_t uint32);
  friend bitstream &operator<<(bitstream &b, uint64_t uint64);
  friend bitstream &operator<<(bitstream &b, const std::string &s);
  friend bitstream &operator<<(bitstream &b, int32_t int32);
};
std::ostream &operator<<(std::ostream &o, bitstream &b) {
  for (auto i : b.buffer) {
    o << (char)i;
  }
  b.buffer.clear();
  return o;
}
bitstream &operator<<(bitstream &b, uint8_t uint8) {
  b.buffer.push_back(uint8);
  return b;
}
bitstream &operator<<(bitstream &b, const uint16_t uint16) {
  auto *c = (uint8_t *)&uint16;
  b.buffer.push_back(c[0]);
  b.buffer.push_back(c[1]);
  return b;
}
bitstream &operator<<(bitstream &b, const uint32_t uint32) {
  auto *c = (uint8_t *)&uint32;
  b.buffer.push_back(c[0]);
  b.buffer.push_back(c[1]);
  b.buffer.push_back(c[2]);
  b.buffer.push_back(c[3]);
  return b;
}
bitstream &operator<<(bitstream &b, uint64_t uint64) {
  auto *c = (uint8_t *)&uint64;
  b.buffer.push_back(c[0]);
  b.buffer.push_back(c[1]);
  b.buffer.push_back(c[2]);
  b.buffer.push_back(c[3]);
  b.buffer.push_back(c[4]);
  b.buffer.push_back(c[5]);
  b.buffer.push_back(c[6]);
  b.buffer.push_back(c[7]);
  return b;
}
bitstream &operator<<(bitstream &b, int32_t int32) {
  auto *c = (uint8_t *)&int32;
  b.buffer.push_back(c[0]);
  b.buffer.push_back(c[1]);
  b.buffer.push_back(c[2]);
  b.buffer.push_back(c[3]);
  return b;
}
bitstream &operator<<(bitstream &b, const std::string &s) {
  for (auto i : s) {
    b.buffer.push_back(i);
  }
  return b;
}
} // namespace bio

#endif // LSM_KV_BITSTREAM_HPP
