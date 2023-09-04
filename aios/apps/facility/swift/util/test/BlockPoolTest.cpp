#include "swift/util/BlockPool.h"

#include <algorithm>
#include <functional>
#include <iosfwd>
#include <memory>
#include <stdint.h>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/Thread.h"
#include "autil/TimeUtility.h"
#include "swift/util/Block.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;

namespace swift {
namespace util {

class BlockPoolTest : public TESTBASE {};

TEST_F(BlockPoolTest, testSimpleProcess) {
    BlockPool pool(201, 10);
    EXPECT_TRUE(pool.allocate());

    EXPECT_EQ(int64_t(21), pool.getMaxBlockCount());
    EXPECT_EQ(int64_t(0), pool.getUsedBlockCount());
    EXPECT_EQ(int64_t(10), pool.getBlockSize());
    vector<BlockPtr> v;
    for (int i = 0; i < 23; ++i) {
        BlockPtr b = pool.allocate();
        if (i < 21) {
            EXPECT_TRUE(b);
            v.push_back(b);
        } else {
            EXPECT_TRUE(!b);
        }
        EXPECT_EQ(int64_t(v.size()), pool.getUsedBlockCount());
    }

    EXPECT_EQ(int64_t(21), int64_t(v.size()));
    v.clear();
    EXPECT_EQ(int64_t(v.size()), pool.getUsedBlockCount());
    EXPECT_TRUE(pool.allocate());
}

void allocateFun(BlockPool *pool, int64_t allocateTimes) {
    vector<BlockPtr> v;
    for (int64_t i = 0; i < allocateTimes; ++i) {
        BlockPtr b = pool->allocate();
        if (b) {
            v.push_back(b);
        }
        if (!v.empty()) {
            if (i % 2) {
                v.pop_back();
            }
        }
    }
}

TEST_F(BlockPoolTest, testMultiThread) {
    BlockPool pool(2001, 10);

    vector<ThreadPtr> v;
    for (int i = 0; i < 8; ++i) {
        v.push_back(Thread::createThread(bind(&allocateFun, &pool, 100000)));
    }
    v.clear();
    EXPECT_EQ(int64_t(0), pool.getUsedBlockCount());
}

TEST_F(BlockPoolTest, testReuseOldBlockBuffer) {
    int64_t bufferSize = 10;
    int64_t blockSize = 10;
    BlockPool pool(bufferSize, blockSize); // only one block
    BlockPtr blockPtr = pool.allocate();
    char *bufAddress = blockPtr->getBuffer();
    blockPtr.reset();

    // allocate again
    blockPtr = pool.allocate();
    char *newBufAddress = blockPtr->getBuffer();
    EXPECT_EQ(bufAddress, newBufAddress);
}

TEST_F(BlockPoolTest, testAllocateWithParentPool) {
    BlockPool *parentPool = new BlockPool(200, 20);
    BlockPool *pool1 = new BlockPool(parentPool, 100, 20);
    BlockPool *pool2 = new BlockPool(parentPool, 120, 40);
    vector<BlockPtr> v1;
    BlockPtr b;
    for (int i = 1; i <= 5; ++i) {
        b = pool1->allocate();
        EXPECT_TRUE(b);
        v1.push_back(b);
        EXPECT_EQ(int64_t(i), pool1->getUsedBlockCount());
        EXPECT_EQ(int64_t(i), parentPool->getUsedBlockCount());
        EXPECT_EQ(int64_t(20), b->getSize());
    }
    b = pool1->allocate();
    EXPECT_TRUE(!b);

    vector<BlockPtr> v2;
    for (int i = 1; i <= 5; ++i) {
        b = pool2->allocate();
        EXPECT_TRUE(b);
        v2.push_back(b);
        EXPECT_EQ(int64_t(i), pool2->getUsedBlockCount());
        EXPECT_EQ(int64_t(i) + pool1->getUsedBlockCount(), parentPool->getUsedBlockCount());
        EXPECT_EQ(int64_t(20), b->getSize());
    }
    b = pool2->allocate();
    EXPECT_TRUE(!b);
    // free one block in pool_1, then pool_1 can allocated
    v1.pop_back();
    EXPECT_EQ(int64_t(v1.size()), pool1->getUsedBlockCount());
    EXPECT_EQ(int64_t(1), pool1->getUnusedBlockCount());
    EXPECT_EQ(int64_t(10), parentPool->getUsedBlockCount());
    b = pool2->allocate();
    EXPECT_TRUE(!b);
    b = pool1->allocate();
    EXPECT_TRUE(b);
    // free unused block
    b.reset();
    EXPECT_EQ(int64_t(1), pool1->getUnusedBlockCount());
    EXPECT_EQ(int64_t(0), parentPool->getUnusedBlockCount());
    pool1->freeUnusedBlock();
    EXPECT_EQ(int64_t(0), pool1->getUnusedBlockCount());
    EXPECT_EQ(int64_t(1), parentPool->getUnusedBlockCount());
    v1.clear();
    EXPECT_EQ(int64_t(4), pool1->getUnusedBlockCount());
    EXPECT_EQ(int64_t(1), parentPool->getUnusedBlockCount());
    pool1->freeUnusedBlock();
    EXPECT_EQ(int64_t(1), pool1->getUnusedBlockCount()); // min buffer
    EXPECT_EQ(int64_t(4), parentPool->getUnusedBlockCount());
    // delele pool
    DELETE_AND_SET_NULL(pool1);
    EXPECT_EQ(int64_t(5), parentPool->getUsedBlockCount());
    b = pool2->allocate();
    EXPECT_TRUE(b);
    v2.push_back(b);
    b.reset();
    EXPECT_EQ(int64_t(v2.size()), pool2->getUsedBlockCount());
    v2.clear();
    DELETE_AND_SET_NULL(pool2);
    EXPECT_EQ(int64_t(0), parentPool->getUsedBlockCount());
    EXPECT_EQ(int64_t(10), parentPool->getUnusedBlockCount());
    parentPool->setReserveBlockCount(3);
    parentPool->freeUnusedBlock();
    EXPECT_EQ(int64_t(3), parentPool->getUnusedBlockCount());
    DELETE_AND_SET_NULL(parentPool);
}

} // namespace util
} // namespace swift
