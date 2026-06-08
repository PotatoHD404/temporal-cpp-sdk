#include <string>

#include <gtest/gtest.h>

#include "internal/lru_cache.h"

namespace {

using temporal::internal::LruCache;

TEST(LruCache, UnboundedKeepsEverything) {
  LruCache<int, int> c(0);
  for (int i = 0; i < 100; ++i) {
    c.Put(i, i * 10);
  }
  EXPECT_EQ(c.size(), 100U);
  ASSERT_NE(c.Get(0), nullptr);
  EXPECT_EQ(*c.Get(0), 0);
  EXPECT_EQ(*c.Get(99), 990);
}

TEST(LruCache, EvictsLeastRecentlyUsedOnOverflow) {
  LruCache<std::string, int> c(2);
  c.Put("a", 1);
  c.Put("b", 2);
  c.Put("c", 3);  // evicts "a" (least recently used)
  EXPECT_EQ(c.size(), 2U);
  EXPECT_FALSE(c.Contains("a"));
  EXPECT_TRUE(c.Contains("b"));
  EXPECT_TRUE(c.Contains("c"));
}

TEST(LruCache, GetMarksMostRecentlyUsed) {
  LruCache<std::string, int> c(2);
  c.Put("a", 1);
  c.Put("b", 2);
  ASSERT_NE(c.Get("a"), nullptr);  // "a" becomes most-recently-used
  c.Put("c", 3);                   // evicts "b" (now LRU), keeps "a"
  EXPECT_TRUE(c.Contains("a"));
  EXPECT_FALSE(c.Contains("b"));
  EXPECT_TRUE(c.Contains("c"));
}

TEST(LruCache, PutUpdatesExistingAndMarksMru) {
  LruCache<std::string, int> c(2);
  c.Put("a", 1);
  c.Put("b", 2);
  c.Put("a", 11);  // update "a" + mark most-recently-used
  ASSERT_NE(c.Get("a"), nullptr);
  EXPECT_EQ(*c.Get("a"), 11);
  c.Put("c", 3);  // evicts "b"
  EXPECT_TRUE(c.Contains("a"));
  EXPECT_FALSE(c.Contains("b"));
}

TEST(LruCache, EraseRemoves) {
  LruCache<std::string, int> c(0);
  c.Put("a", 1);
  c.Erase("a");
  EXPECT_FALSE(c.Contains("a"));
  EXPECT_EQ(c.Get("a"), nullptr);
  EXPECT_EQ(c.size(), 0U);
}

TEST(LruCache, GetAbsentReturnsNull) {
  LruCache<int, int> c(2);
  EXPECT_EQ(c.Get(42), nullptr);
}

}  // namespace
