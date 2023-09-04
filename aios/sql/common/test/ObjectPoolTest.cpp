#include "sql/common/ObjectPool.h"

#include <deque>
#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

#include "unittest/unittest.h"

using namespace std;

namespace sql {

namespace {

template <class T>
struct ReuseObject {
    using ValueType = T;

    T _data;

    void clearObject() {
        _data.clear();
    }
};

using ReuseString = ReuseObject<string>;
using ReuseVector = ReuseObject<vector<int>>;

} // namespace

class ObjectPoolTest : public ::testing::Test {
public:
    template <class T>
    shared_ptr<T> getObject(const string &key) {
        auto creator = []() { return new T; };
        return _pool.get<T>(key, creator);
    }

    template <class T>
    void fakeUsage(const shared_ptr<T> &ptr, const typename T::ValueType &data) {
        ASSERT_TRUE(ptr != nullptr);
        ptr->_data = data;
    }

protected:
    ObjectPool _pool;
};

TEST_F(ObjectPoolTest, testUsage) {
    ReuseString *p1 = nullptr;
    ReuseString *p2 = nullptr;
    {
        auto ptr1 = getObject<ReuseString>("aaa");
        ASSERT_NO_FATAL_FAILURE(fakeUsage(ptr1, "111"));
        auto ptr2 = getObject<ReuseString>("aaa");
        ASSERT_NO_FATAL_FAILURE(fakeUsage(ptr2, "222"));
        EXPECT_EQ("111", ptr1->_data);
        EXPECT_EQ("222", ptr2->_data);
        p1 = ptr1.get();
        p2 = ptr2.get();
    }
    {
        auto ptr1 = getObject<ReuseString>("aaa");
        auto ptr2 = getObject<ReuseString>("aaa");
        ASSERT_EQ(p1, ptr1.get());
        ASSERT_EQ(p2, ptr2.get());
        EXPECT_TRUE(ptr1->_data.empty());
        EXPECT_TRUE(ptr2->_data.empty());
    }
}

TEST_F(ObjectPoolTest, testDiffTypeSameKey) {
    ReuseString *strP1 = nullptr;
    ReuseString *strP2 = nullptr;
    ReuseVector *vecP1 = nullptr;
    ReuseVector *vecP2 = nullptr;
    {
        auto strPtr1 = getObject<ReuseString>("aaa");
        auto strPtr2 = getObject<ReuseString>("aaa");
        auto vecPtr1 = getObject<ReuseVector>("aaa");
        auto vecPtr2 = getObject<ReuseVector>("aaa");
        strP1 = strPtr1.get();
        strP2 = strPtr2.get();
        vecP1 = vecPtr1.get();
        vecP2 = vecPtr2.get();
    }
    {
        auto strPtr1 = getObject<ReuseString>("aaa");
        ASSERT_NO_FATAL_FAILURE(fakeUsage(strPtr1, "111"));
        auto strPtr2 = getObject<ReuseString>("aaa");
        ASSERT_NO_FATAL_FAILURE(fakeUsage(strPtr2, "222"));
        auto vecPtr1 = getObject<ReuseVector>("aaa");
        ASSERT_NO_FATAL_FAILURE(fakeUsage(vecPtr1, {1, 2, 3}));
        auto vecPtr2 = getObject<ReuseVector>("aaa");
        ASSERT_NO_FATAL_FAILURE(fakeUsage(vecPtr2, {4, 5}));

        EXPECT_EQ(strP1, strPtr1.get());
        EXPECT_EQ(strP2, strPtr2.get());
        EXPECT_EQ(vecP1, vecPtr1.get());
        EXPECT_EQ(vecP2, vecPtr2.get());

        EXPECT_EQ("111", strPtr1->_data);
        EXPECT_EQ("222", strPtr2->_data);
        using VecType = ReuseVector::ValueType;
        EXPECT_EQ(VecType({1, 2, 3}), vecPtr1->_data);
        EXPECT_EQ(VecType({4, 5}), vecPtr2->_data);
    }
}

TEST_F(ObjectPoolTest, testMapSizeLimit) {
    _pool.maxSize = 1;
    {
        auto ptr1 = getObject<ReuseString>("aaa");
        auto ptr2 = getObject<ReuseString>("aaa");
    }
    {
        auto ptr1 = getObject<ReuseString>("aaa");
        ASSERT_NO_FATAL_FAILURE(fakeUsage(ptr1, "111"));
        auto ptr2 = getObject<ReuseString>("bbb");
        ptr2.reset();
        EXPECT_EQ("111", ptr1->_data);
    }
}

TEST_F(ObjectPoolTest, testSlotSizeLimit) {
    getObject<ReuseString>("aaa");
    _pool.reverse<ReuseString>("aaa", 1);
    {
        auto ptr1 = getObject<ReuseString>("aaa");
        auto ptr2 = getObject<ReuseString>("aaa");
    }
    EXPECT_EQ(1, _pool.getKeySize<ReuseString>("aaa"));
    auto ptr = getObject<ReuseString>("aaa");
    EXPECT_EQ(0, _pool.getKeySize<ReuseString>("aaa"));
    ptr.reset();
    EXPECT_EQ(1, _pool.getKeySize<ReuseString>("aaa"));
}

} // namespace sql
