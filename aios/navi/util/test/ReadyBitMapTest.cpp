#include "navi/util/ReadyBitMap.h"

#include "autil/Thread.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace navi {

class ReadyBitMapTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void ReadyBitMapTest::setUp() {
}

void ReadyBitMapTest::tearDown() {
}

TEST_F(ReadyBitMapTest, testConstruct) {
    {
        auto size = UNIT / 2;
        BitState16 map(size);
        for (size_t i = 0; i < size; i++) {
            EXPECT_TRUE(map.isFinish(i));
            EXPECT_FALSE(map.isReady(i));
            EXPECT_FALSE(map.isOptional(i));
        }
        for (size_t i = 0; i < size; i++) {
            EXPECT_TRUE(map.isFinish(i));
            EXPECT_FALSE(map.isReady(i));
            EXPECT_FALSE(map.isOptional(i));
        }
    }
    {
        BitState16 map(UNIT);
        for (size_t i = 10; i < UNIT; i++) {
            EXPECT_TRUE(map.isFinish(i));
            EXPECT_FALSE(map.isReady(i));
            EXPECT_FALSE(map.isOptional(i));
        }
        for (size_t i = 10; i < UNIT; i++) {
            EXPECT_TRUE(map.isFinish(i));
            EXPECT_FALSE(map.isReady(i));
            EXPECT_FALSE(map.isOptional(i));
        }
    }
}

TEST_F(ReadyBitMapTest, testReadyFinish) {
    for (size_t bitMapSize = 1; bitMapSize < 200; bitMapSize++) {
        auto headBit = 0;
        auto middleBit = bitMapSize / 2;
        auto endBit = bitMapSize - 1;
        set<uint32_t> testSet;
        testSet.insert(headBit);
        testSet.insert(middleBit);
        testSet.insert(endBit);
        auto readyBitMap = ReadyBitMap::createReadyBitMap(bitMapSize);

        for (size_t i = 0; i < bitMapSize; i++) {
            readyBitMap->setFinish(i, false);
        }
        for (auto bit : testSet) {
            EXPECT_FALSE(readyBitMap->isReady(bit));
            EXPECT_FALSE(readyBitMap->isFinish(bit));
        }
        EXPECT_FALSE(readyBitMap->isOk());

        {
            size_t i = 0;
            for (auto bit = testSet.begin(); bit != testSet.end(); bit++, i++) {
                auto index = *bit;
                readyBitMap->setReady(index, true);
                EXPECT_TRUE(readyBitMap->isReady(index));
                EXPECT_FALSE(readyBitMap->isFinish(index));
                if (i < testSet.size() - 1) {
                    EXPECT_FALSE(readyBitMap->isOk());
                }
            }
        }

        for (size_t i = 0; i < bitMapSize; i++) {
            readyBitMap->setReady(i, true);
        }
        EXPECT_TRUE(readyBitMap->isOk());

        for (auto bit : testSet) {
            readyBitMap->setReady(bit, false);
            EXPECT_FALSE(readyBitMap->isOk());
            readyBitMap->setFinish(bit, true);
            EXPECT_TRUE(readyBitMap->isOk());
        }

        for (size_t i = 0; i < bitMapSize; i++) {
            readyBitMap->setReady(i, false);
            readyBitMap->setFinish(i, true);
            EXPECT_TRUE(readyBitMap->isOk());
        }
        EXPECT_TRUE(readyBitMap->isOk());
        ReadyBitMap::freeReadyBitMap(readyBitMap);
    }
}

TEST_F(ReadyBitMapTest, testMultiThread) {
    constexpr uint32_t bitMapSize = 178;
    constexpr uint32_t threadCount = 4;
    std::vector<autil::ThreadPtr> threads;
    auto readyBitMap = ReadyBitMap::createReadyBitMap(bitMapSize);
    std::vector<std::function<void ()>> funcs;
    for (size_t i = 0; i < threadCount; i++) {
        threads.push_back(autil::Thread::createThread(std::bind([=]() {
            for (size_t loop = 0; loop < 10000; loop++) {
                for (size_t index = 0; index < bitMapSize; index++) {
                    uint32_t bit = index % threadCount;
                    if (bit == i) {
                        readyBitMap->setReady(bit, true);
                        ASSERT_TRUE(readyBitMap->isReady(bit));
                        readyBitMap->setReady(bit, false);

                        readyBitMap->setFinish(bit, false);
                        ASSERT_FALSE(readyBitMap->isFinish(bit));
                        readyBitMap->setFinish(bit, true);
                    }
                }
            }
        })));
    }
    threads.clear();
    ReadyBitMap::freeReadyBitMap(readyBitMap);
}

TEST_F(ReadyBitMapTest, testOptionalHasReady) {
    auto *readyBitMap = ReadyBitMap::createReadyBitMap(2);
    readyBitMap->setOptional(0, 1);
    readyBitMap->setOptional(1, 1);
    readyBitMap->setFinish(false);
    readyBitMap->setReady(0, 1);
    readyBitMap->setFinish(0, 1);
    ASSERT_TRUE(readyBitMap->hasReady());
    ASSERT_FALSE(readyBitMap->isReady());
    ASSERT_TRUE(readyBitMap->isOk());
    ASSERT_FALSE(readyBitMap->isFinish());
    ReadyBitMap::freeReadyBitMap(readyBitMap);
}


}
