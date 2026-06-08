#pragma once

#include <cstddef>
#include <list>
#include <unordered_map>
#include <utility>

// A small least-recently-used cache. Bounds the sticky workflow cache so a
// long-lived worker doesn't grow without limit. Not thread-safe — the caller
// holds its own mutex (the workflow task handler already does). A capacity of 0
// means unbounded (no eviction).
namespace temporal::internal {

template <class K, class V>
class LruCache {
 public:
  explicit LruCache(std::size_t capacity = 0) : capacity_(capacity) {}

  // Returns a pointer to the value for `key`, marking it most-recently-used, or
  // nullptr if absent. The pointer is valid until the next mutating call.
  V* Get(const K& key) {
    const auto it = index_.find(key);
    if (it == index_.end()) {
      return nullptr;
    }
    order_.splice(order_.begin(), order_, it->second);  // move to front (MRU)
    return &it->second->second;
  }

  // Insert or replace `key`, marking it most-recently-used, then evict the
  // least-recently-used entries until the capacity is respected.
  void Put(const K& key, V value) {
    const auto it = index_.find(key);
    if (it != index_.end()) {
      it->second->second = std::move(value);
      order_.splice(order_.begin(), order_, it->second);
    } else {
      order_.emplace_front(key, std::move(value));
      index_.emplace(key, order_.begin());
    }
    EvictToCapacity();
  }

  void Erase(const K& key) {
    const auto it = index_.find(key);
    if (it == index_.end()) {
      return;
    }
    order_.erase(it->second);
    index_.erase(it);
  }

  bool Contains(const K& key) const { return index_.find(key) != index_.end(); }
  std::size_t size() const { return index_.size(); }

 private:
  void EvictToCapacity() {
    while (capacity_ != 0 && index_.size() > capacity_) {
      auto& victim = order_.back();
      index_.erase(victim.first);
      order_.pop_back();
    }
  }

  std::size_t capacity_;
  std::list<std::pair<K, V>> order_;  // front = most-recently-used
  std::unordered_map<K, typename std::list<std::pair<K, V>>::iterator> index_;
};

}  // namespace temporal::internal
