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
  [[nodiscard]] int randomLevel() const;
  size_t node_cnt;
  double level_up;
  int lvl;

protected:
  SKNode<KeyType, ValType> *head;

public:
  /**
   * @brief Construct a new Skip List object
   *
   */
  SkipList();
  /**
   * @brief Construct a new Skip List object
   *
   * @param p the probability to goes up a level when insert a key-val
   */
  explicit SkipList(double p);
  SkipList(const SkipList &s);
  ~SkipList();

  void insert(KeyType key, ValType value);
  ValType *search(KeyType key) const;
  void remove(KeyType key);
  std::list<SKNode<KeyType, ValType> *> scan(KeyType key_start,
                                             KeyType key_end) const;
  void display();
  const SKNode<KeyType, ValType> *begin() const;
  const SKNode<KeyType, ValType> *end() const;
  [[nodiscard]] size_t size() const;
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
inline void SkipList<KeyType, ValType>::insert(KeyType key, ValType value) {
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
    if (node_cnt >= level_up) {
      level_up *= p;
      ++lvl;
    }

    auto node_lvl = SkipList::randomLevel();
    auto node_new = new SKNode<KeyType, ValType>{key, value, NORMAL, node_lvl};
    for (int i = 0; i < node_lvl; ++i) {
      node_new->forwards[i] = nodes[i]->forwards[i];
      nodes[i]->forwards[i] = node_new;
    }
  }
  delete[] nodes;
}
template <typename KeyType, typename ValType>
inline ValType *SkipList<KeyType, ValType>::search(KeyType key) const {
  auto node = head;
  for (auto curr_lvl = lvl - 1; curr_lvl >= 0; --curr_lvl) {
#ifdef DEBUG
    print_node(node, curr_lvl);
#endif
    while (node->forwards[curr_lvl]->type != SKNodeType::END &&
           node->forwards[curr_lvl]->key < key) {
      node = node->forwards[curr_lvl];
#ifdef DEBUG
      print_node(node, curr_lvl);
#endif
    }
  }
  node = node->forwards[0];
#ifdef DEBUG
  print_node(node, 0);
#endif
  if (node->key == key) {
#ifdef DEBUG
    std::cout << node->val << std::endl;
#endif
    return &(node->val); // found
  }
  return nullptr;
  // not found
}
template <typename KeyType, typename ValType>
inline void SkipList<KeyType, ValType>::remove(KeyType key) {
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
inline void SkipList<KeyType, ValType>::display() {
  for (int i = MAX_LEVEL - 1; i >= 0; --i) {
#ifdef DEBUG
    std::cout << "Level " << i + 1 << ":h";
#endif
    auto *node = head->forwards[i];
    while (node->type != SKNodeType::END) {
#ifdef DEBUG
      std::cout << "-->(" << node->key << "," << node->val << ")";
#endif
      node = node->forwards[i];
    }
#ifdef _DEBUG
    std::cout << "-->N" << std::endl;
#endif
  }
}
template <typename KeyType, typename ValType>
inline std::list<SKNode<KeyType, ValType> *>
SkipList<KeyType, ValType>::scan(KeyType key, KeyType key_end) const {

  auto node = head;
  for (auto curr_lvl = lvl - 1; curr_lvl >= 0; --curr_lvl) {
    while (node->forwards[curr_lvl]->type != SKNodeType::END &&
           node->forwards[curr_lvl]->key < key) {
      node = node->forwards[curr_lvl];
    }
  }
  node = node->forwards[0];
  std::list<SKNode<KeyType, ValType> *> ans;
  while (node->type != SKNodeType::END && node->key <= key_end) {
    ans.push_back(node);
    node = node->forwards[0];
  }
  return ans;
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
  nil = new SKNode<KeyType, ValType>(KeyType(), ValType(), SKNodeType::END,
                                     MAX_LEVEL);
  head = new SKNode<KeyType, ValType>(KeyType(), ValType(), SKNodeType::HEAD,
                                      MAX_LEVEL, nil);

  for (auto &forward : head->forwards) {
    forward = nil;
  }
  level_up = 1;
  for (int i = 0; i < lvl; ++i) {
    level_up *= p;
  }
}
template <typename KeyType, typename ValType>
size_t SkipList<KeyType, ValType>::size() const {
  return node_cnt;
}
template <typename KeyType, typename ValType>
const SKNode<KeyType, ValType> *SkipList<KeyType, ValType>::begin() const {
  return head->forwards[0];
}
template <typename KeyType, typename ValType>
const SKNode<KeyType, ValType> *SkipList<KeyType, ValType>::end() const {
  return nil;
}
template <typename KeyType, typename ValType>
SkipList<KeyType, ValType>::SkipList(const SkipList &) {}
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