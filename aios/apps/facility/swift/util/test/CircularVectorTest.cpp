#include "swift/util/CircularVector.h"

#include <stdint.h>

#include "unittest/unittest.h"

namespace swift {
namespace util {

class CircularVectorTest : public TESTBASE {};

TEST_F(CircularVectorTest, testQueue) {
    const uint32_t CAPACITY = 8;
    CircularVector<uint32_t> cq(CAPACITY); // default 10
    EXPECT_EQ(0, cq.size());
    EXPECT_EQ(CAPACITY, cq.capacity());
    EXPECT_TRUE(cq.empty());
    EXPECT_EQ(1, cq.end());
    for (int i = 0; i < 100; i++) {
        cq.push_back(i + 1);
        EXPECT_EQ((i + 2) % CAPACITY, cq.end());
        EXPECT_EQ(i + 1, cq.size());
        EXPECT_EQ(CAPACITY, cq.capacity());
        EXPECT_EQ(i + 1, cq.back());
        if (i > 0) {
            EXPECT_EQ(i, cq.get(i));
        }
        EXPECT_FALSE(cq.empty());
    }
}

TEST_F(CircularVectorTest, testMove) {
    CircularVector<uint32_t> cq;
    EXPECT_EQ(0, cq.size());
    EXPECT_EQ(10, cq.capacity());
    for (int i = 0; i < 10; i++) {
        cq.push_back(i);
        if (i < 8) {
            EXPECT_EQ(i + 2, cq.end());
        } else {
            EXPECT_EQ(i + 2 - 10, cq.end());
        }
    }
    uint32_t pos = cq.forward(3);
    EXPECT_EQ(7, pos);
    EXPECT_EQ(8, cq.next(pos));
    EXPECT_EQ(0, cq.next(pos, 3));
}

} // namespace util
} // namespace swift
