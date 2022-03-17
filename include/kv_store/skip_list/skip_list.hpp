#ifndef SKIP_LIST_SKIP_LIST_HPP
#define SKIP_LIST_SKIP_LIST_HPP
#include <climits>
#include <ctime>
#include <iostream>
#include <random>
#include <vector>

enum SKNodeType { HEAD, NORMAL, END };
template <typename KeyType, typename ValType> struct SKNode;
template <typename KeyType, typename ValType> class SkipList;

inline double rand_();
template <typename K, typename V>
inline void print_node(SKNode<K, V> *node, int lvl);

template <typename KeyType, typename ValType> struct SKNode {
  // enum { MAX_LEVEL = 128 };
  KeyType key;
  ValType val;
  SKNodeType type;
  std::vector<SKNode *> forwards;
  SKNode(KeyType _key, ValType _val, SKNodeType _type, int lvl);
  SKNode(KeyType _key, ValType _val, SKNodeType _type, int lvl, SKNode *ep);
};

template <typename KeyType, typename ValType> class SkipList {
private:
  enum { INIT_LEVEL = 1, MAX_LEVEL = 32 };
  const double p;
  SKNode<KeyType, ValType> *nil;
  int randomLevel() const;
  int node_cnt;
  double level_up;

protected:
  SKNode<KeyType, ValType> *head;

public:
  int lvl;
  /**
   * @brief Construct a new Skip List object
   *
   */
  SkipList();
  /**
   * @brief Construct a new Skip List object
   *
   * @param p
   */
  explicit SkipList(double p);
  void Insert(KeyType key, ValType value);
  void Search(KeyType key);
  void Delete(KeyType key);
  void RegionSearch(KeyType key_start, KeyType key_end);
  void Display();
  void TryLevelUp();
  ~SkipList();
};

inline double rand_() {
  std::random_device dev;
  std::mt19937 rng(dev());
  std::uniform_int_distribution<std::mt19937::result_type> key_scope(1, 1000);
  auto r = key_scope(rng) / 1000.0;
  return r;
}
template <typename K, typename V>
inline void print_node(SKNode<K, V> *node, int lvl) {
  std::cout << lvl + 1 << ',';
  if (node->type == NORMAL) {
    std::cout << node->key;
  } else if (node->type == HEAD) {
    std::cout << 'h';
  } else {
    std::cout << 'N';
  }
  std::cout << ' ';
}
template <typename KeyType, typename ValType>
inline int SkipList<KeyType, ValType>::randomLevel() const {
  int result = 1;
  while (result < MAX_LEVEL && rand_() * p < 1) {
    ++result;
  }
  return result;
}
template <typename KeyType, typename ValType>
inline void SkipList<KeyType, ValType>::Insert(KeyType key, ValType value) {
  auto nodes = new SKNode<KeyType, ValType> *[MAX_LEVEL];
  auto node = head;
  for (auto curr_lvl = MAX_LEVEL - 1; curr_lvl >= 0; --curr_lvl) {
    while (node->forwards[curr_lvl]->type != SKNodeType::END &&
           node->forwards[curr_lvl]->key < key) {
      node = node->forwards[curr_lvl];
    }
    nodes[curr_lvl] = node;
  }
  node = node->forwards[0];
  if (node->type == NORMAL && node->key == key) {
    node->val = value;
  } else {
    ++node_cnt;
    TryLevelUp();

    auto node_lvl = SkipList::randomLevel();
    auto node_new = new SKNode<KeyType, ValType>{key, value, NORMAL, node_lvl};
    for (int i = 0; i < node_lvl; ++i) {
      node_new->forwards[i] = nodes[i]->forwards[i];
      nodes[i]->forwards[i] = node_new;
    }
  }
}
template <typename KeyType, typename ValType>
inline void SkipList<KeyType, ValType>::Search(KeyType key) {
  auto node = head;
  for (auto curr_lvl = lvl - 1; curr_lvl >= 0; --curr_lvl) {
    print_node(node, curr_lvl);
    while (node->forwards[curr_lvl]->type != SKNodeType::END &&
           node->forwards[curr_lvl]->key < key) {
      node = node->forwards[curr_lvl];
      print_node(node, curr_lvl);
    }
  }
  node = node->forwards[0];
  print_node(node, 0);
  if (node->key == key) {
    std::cout << node->val << std::endl;
    return; // found
  }
  std::cout << "Not Found" << std::endl;
  // not found
}
template <typename KeyType, typename ValType>
inline void SkipList<KeyType, ValType>::Delete(KeyType key) {
  auto nodes = new SKNode<KeyType, ValType> *[MAX_LEVEL];
  auto node = head;
  for (int curr_lvl = MAX_LEVEL; curr_lvl >= 0; --curr_lvl) {
    while (node->forwards[curr_lvl]->type != SKNodeType::END &&
           node->forwards[curr_lvl]->key < key) {
      node = node->forwards[curr_lvl];
    }
    nodes[curr_lvl] = node;
  }
  node = node->forwards[0];
  if (node->key == key) {
    for (int i = 0; i < MAX_LEVEL; ++i) {
      if (nodes[i]->forwards[i] != node) {
        break; // finished
      }
      nodes[i]->forwards[i] = nodes[i]->forwards[i]->forwards[i];
    }
    delete node;
  }
}
template <typename KeyType, typename ValType>
inline void SkipList<KeyType, ValType>::Display() {
  for (int i = MAX_LEVEL - 1; i >= 0; --i) {
    std::cout << "Level " << i + 1 << ":h";
    auto *node = head->forwards[i];
    while (node->type != SKNodeType::END) {
      std::cout << "-->(" << node->key << "," << node->val << ")";
      node = node->forwards[i];
    }

    std::cout << "-->N" << std::endl;
  }
}
template <typename KeyType, typename ValType>
inline void SkipList<KeyType, ValType>::RegionSearch(KeyType key,
                                                     KeyType key_end) {

  auto node = head;
  for (auto curr_lvl = lvl - 1; curr_lvl >= 0; --curr_lvl) {
    while (node->forwards[curr_lvl]->type != SKNodeType::END &&
           node->forwards[curr_lvl]->key < key) {
      node = node->forwards[curr_lvl];
    }
  }
  node = node->forwards[0];
  while (node->type != SKNodeType::END && node->key < key_end) {
    node = node->forwards[0];
  }
}
template <typename KeyType, typename ValType>
inline SkipList<KeyType, ValType>::SkipList()
    : node_cnt(0), p(2), lvl(INIT_LEVEL) {
  head = new SKNode<KeyType, ValType>(0, 0, SKNodeType::HEAD, MAX_LEVEL);
  nil = new SKNode<KeyType, ValType>(INT_MAX, 0, SKNodeType::END, MAX_LEVEL);
  for (auto &forward : head->forwards) {
    forward = nil;
  }
  level_up = 1;
  for (int i = 0; i < lvl; ++i) {
    level_up *= p;
  }
}
template <typename KeyType, typename ValType>
inline SkipList<KeyType, ValType>::~SkipList() {
  auto *n1 = head;
  decltype(head) n2;
  while (n1) {
    n2 = n1->forwards[0];
    delete n1;
    n1 = n2;
  }
}
template <typename KeyType, typename ValType>
inline SkipList<KeyType, ValType>::SkipList(double p)
    : node_cnt(0), p(p), lvl(INIT_LEVEL) {
  nil = new SKNode<KeyType, ValType>(INT_MAX, 0, SKNodeType::END, MAX_LEVEL);
  head = new SKNode<KeyType, ValType>(0, 0, SKNodeType::HEAD, MAX_LEVEL, nil);

  for (auto &forward : head->forwards) {
    forward = nil;
  }
  level_up = 1;
  for (int i = 0; i < lvl; ++i) {
    level_up *= p;
  }
}
template <typename KeyType, typename ValType>
inline void SkipList<KeyType, ValType>::TryLevelUp() {
  if (node_cnt >= level_up) {
    level_up *= p;
    ++lvl;
  }
}
template <typename KeyType, typename ValType>
inline SKNode<KeyType, ValType>::SKNode(KeyType _key, ValType _val,
                                        SKNodeType _type, int lvl)
    : key(_key), val(_val), type(_type) {
  forwards.resize(lvl, nullptr);
}
template <typename KeyType, typename ValType>
inline SKNode<KeyType, ValType>::SKNode(KeyType _key, ValType _val,
                                        SKNodeType _type, int lvl, SKNode *ep)
    : key(_key), val(_val), type(_type) {
  forwards.resize(lvl, ep);
}
#endif