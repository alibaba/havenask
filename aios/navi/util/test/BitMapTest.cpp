#include "unittest/unittest.h"
#include "navi/util/BitMap.h"
#include "autil/Thread.h"

using namespace std;
using namespace testing;

namespace navi {

class BitMapTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void BitMapTest::setUp() {
}

void BitMapTest::tearDown() {
}

TEST_F(BitMapTest, testBitMap16Construct) {
    auto size = BIT_UNIT / 2;
    BitMap16 set(size);

    EXPECT_FALSE(set.all());
    EXPECT_TRUE(set.none());
    EXPECT_FALSE(set.any());
    for (size_t i = 0; i < size; i++) {
        EXPECT_FALSE(set[i]);
    }
    for (size_t i = 0; i < BIT_UNIT; i++) {
        EXPECT_FALSE(set[i]);
    }
}

TEST_F(BitMapTest, testBitMap16Set) {
    auto size = BIT_UNIT / 2;
    BitMap16 set(size);

    EXPECT_FALSE(set.all());
    EXPECT_TRUE(set.none());
    EXPECT_FALSE(set.any());

    set.set();

    EXPECT_TRUE(set.all());
    EXPECT_FALSE(set.none());
    EXPECT_TRUE(set.any());
}


TEST_F(BitMapTest, testBitMap16SetValue) {
    auto size = BIT_UNIT / 4;
    BitMap16 set(size);

    EXPECT_FALSE(set.all());
    EXPECT_TRUE(set.none());
    EXPECT_FALSE(set.any());

    EXPECT_FALSE(set[0]);
    set.set(0, true);
    EXPECT_TRUE(set[0]);

    EXPECT_FALSE(set.all());
    EXPECT_FALSE(set.none());
    EXPECT_TRUE(set.any());

    EXPECT_FALSE(set[1]);
    set.set(1, true);
    EXPECT_TRUE(set[1]);
    EXPECT_FALSE(set.all());
    EXPECT_FALSE(set.none());
    EXPECT_TRUE(set.any());

    EXPECT_FALSE(set[2]);
    set.set(2, true);
    EXPECT_TRUE(set[2]);
    EXPECT_FALSE(set.all());
    EXPECT_FALSE(set.none());
    EXPECT_TRUE(set.any());

    EXPECT_FALSE(set[3]);
    set.set(3, true);
    EXPECT_TRUE(set[3]);
    EXPECT_TRUE(set.all());
    EXPECT_FALSE(set.none());
    EXPECT_TRUE(set.any());

}

TEST_F(BitMapTest, testBitMap16Reset) {
    auto size = BIT_UNIT / 2;
    BitMap16 set(size);

    EXPECT_FALSE(set.all());
    EXPECT_TRUE(set.none());
    EXPECT_FALSE(set.any());

    set.set();

    EXPECT_TRUE(set.all());
    EXPECT_FALSE(set.none());
    EXPECT_TRUE(set.any());
}


TEST_F(BitMapTest, testBitMapConstruct) {
    size_t size = 77;
    BitMap set(size);

    EXPECT_FALSE(set.all());
    EXPECT_TRUE(set.none());
    EXPECT_FALSE(set.any());

    for (size_t i = 0; i < size; i++) {
        EXPECT_FALSE(set[i]);
    }
    EXPECT_EQ(size, set.size());
    EXPECT_EQ(0, set.count());
}


TEST_F(BitMapTest, testBitMapCount) {
    size_t size = 10;
    BitMap set(size);
    set.set(0, true);
    EXPECT_EQ(1, set.count());
    EXPECT_EQ("0000000001", set.toString());

    set.set(1, true);
    EXPECT_EQ(2, set.count());
    EXPECT_EQ("0000000011", set.toString());

    set.set(1, false);
    EXPECT_EQ(1, set.count());
    EXPECT_EQ("0000000001", set.toString());

    set.set(1, true);
    EXPECT_EQ(2, set.count());
    EXPECT_EQ("0000000011", set.toString());

    set.set(2, true);
    EXPECT_EQ(3, set.count());
    EXPECT_EQ("0000000111", set.toString());

    set.set(3, true);
    EXPECT_EQ(4, set.count());
    EXPECT_EQ("0000001111", set.toString());

    set.set(4, true);
    EXPECT_EQ(5, set.count());
    EXPECT_EQ("0000011111", set.toString());

    set.reset();
    EXPECT_EQ(0, set.count());
    EXPECT_EQ("0000000000", set.toString());

    set.set();
    EXPECT_EQ(size, set.count());
    EXPECT_EQ("1111111111", set.toString());


    BitMap set2(20);
    set2.set(0, true);
    set2.set(16, true);
    EXPECT_EQ(2, set2.count());
    EXPECT_EQ("00010000000000000001", set2.toString());

}

TEST_F(BitMapTest, testMultiThread) {
    constexpr uint32_t bitMapSize = 4;
    constexpr uint32_t threadCount = 4;

    BitMap bitMap(bitMapSize);
    std::vector<autil::ThreadPtr> threads;
    for (size_t i = 0; i < threadCount; i++) {
        threads.push_back(autil::Thread::createThread(std::bind([&bitMap, i]() {
            for (size_t loop = 0; loop < 10000; loop++) {
                for (size_t index = 0; index < bitMapSize; index++) {
                    uint32_t bit = index % threadCount;
                    if (i == bit) {
                        bitMap.set(bit, true);
                        ASSERT_TRUE(bitMap[bit]);
                        bitMap.set(bit, false);
                        ASSERT_FALSE(bitMap[bit]);
                    }
                }
            }
        })));
    }
    threads.clear();
}

}
