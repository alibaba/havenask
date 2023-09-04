#include "swift/util/ObjectLruCache.h"

#include <cstddef>
#include <string>

#include "unittest/unittest.h"

using namespace std;
namespace swift {
namespace util {

class ObjectLruCacheTest : public TESTBASE {};

TEST_F(ObjectLruCacheTest, testCacheSize) {
    ObjectLruCache<string, string> cache;
    string key, val;
    EXPECT_EQ(size_t(0), cache.size());
    EXPECT_TRUE(cache.empty());
    EXPECT_TRUE(!cache.back(key, val));
    cache.put("key1", "value1");
    EXPECT_EQ(size_t(1), cache.size());
    EXPECT_TRUE(!cache.empty());
    EXPECT_TRUE(cache.back(key, val));
    EXPECT_EQ(string("key1"), key);
    EXPECT_EQ(string("value1"), val);
    cache.clear();
    EXPECT_EQ(size_t(0), cache.size());
    EXPECT_TRUE(cache.empty());
    EXPECT_TRUE(!cache.back(key, val));
}

TEST_F(ObjectLruCacheTest, testCachePutAndGet) {
    ObjectLruCache<string, string> cache;
    string key, val;
    cache.put("key3", "value3");
    cache.put("key2", "value2");
    cache.put("key1", "value1");
    EXPECT_TRUE(cache.back(key, val));
    EXPECT_EQ(string("key3"), key);
    EXPECT_EQ(string("value3"), val);
    EXPECT_NEAR(0.0, cache.hitRatio(), 0.0001);

    EXPECT_TRUE(cache.get("key3", val));
    EXPECT_EQ(string("value3"), val);
    EXPECT_NEAR(1.0, cache.hitRatio(), 0.0001);

    EXPECT_TRUE(cache.back(key, val));
    EXPECT_EQ(string("key2"), key);
    EXPECT_EQ(string("value2"), val);

    EXPECT_TRUE(cache.pop());
    EXPECT_TRUE(cache.back(key, val));
    EXPECT_EQ(string("key1"), key);
    EXPECT_EQ(string("value1"), val);
    EXPECT_TRUE(!cache.get("key2", val));
    EXPECT_NEAR(0.5, cache.hitRatio(), 0.0001);
    cache.clear();
    EXPECT_TRUE(!cache.get("key1", val));
    EXPECT_NEAR(0.3333, cache.hitRatio(), 0.0001);
    EXPECT_TRUE(!cache.get("key3", val));
    EXPECT_NEAR(0.25, cache.hitRatio(), 0.0001);
}

TEST_F(ObjectLruCacheTest, testRemove) {
    ObjectLruCache<string, string> cache;
    string key, val;
    cache.put("key3", "value3");
    cache.put("key3", "value4");
    cache.put("key2", "value2");
    cache.put("key2", "value5");
    cache.put("key1", "value1");
    EXPECT_TRUE(cache.get("key3", val));
    EXPECT_EQ(string("value4"), val);
    EXPECT_EQ(size_t(3), cache.size());
    cache.remove("key3");
    EXPECT_EQ(size_t(2), cache.size());
    EXPECT_TRUE(!cache.get("key3", val));
    EXPECT_TRUE(cache.get("key2", val));
    EXPECT_EQ(string("value5"), val);
    cache.remove("key2");
    EXPECT_EQ(size_t(1), cache.size());
    cache.remove("key1");
    EXPECT_EQ(size_t(0), cache.size());
    EXPECT_TRUE(cache.empty());
}

} // namespace util
} // namespace swift
