#include "swift/util/ObjectBlock.h"

#include <stdint.h>

#include "swift/util/Block.h"
#include "unittest/unittest.h"

namespace swift {
namespace util {

class ObjectBlockTest : public TESTBASE {};

TEST_F(ObjectBlockTest, testSimpleProcess) {
    {
        BlockPtr block(new Block(101));
        ObjectBlock<int32_t> objectBlock(block);
        EXPECT_EQ(int64_t(101 / sizeof(int32_t)), objectBlock.size());
        for (int64_t i = 0; i < objectBlock.size(); i++) {
            objectBlock[i] = i;
        }
        for (int64_t i = 0; i < objectBlock.size(); i++) {
            EXPECT_EQ((int32_t)i, objectBlock[i]);
        }
        objectBlock.destoryBefore(10);
        objectBlock.destoryBefore(5);
        for (int64_t i = 11; i < objectBlock.size(); i++) {
            EXPECT_EQ((int32_t)i, objectBlock[i]);
        }
    }
}

} // namespace util
} // namespace swift
