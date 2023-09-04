#include "swift/common/MemoryMessage.h"

#include <iosfwd>
#include <stdint.h>

#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace swift {
namespace common {

class MemoryMessageTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void MemoryMessageTest::setUp() {}

void MemoryMessageTest::tearDown() {}

TEST_F(MemoryMessageTest, testMsgLen_Count_Offset) {
    {
        MemoryMessage msg;
        msg.setLen(10);
        msg.setOffset(9);
        msg.setMsgCount(8);
        ASSERT_EQ(10, msg.getLen());
        ASSERT_EQ(9, msg.getOffset());
        ASSERT_EQ(8, msg.getMsgCount());
    }
    {
        MemoryMessage msg;
        uint64_t maxLen = (1 << 27) - 1;
        uint64_t maxOffset = (1 << 25) - 1;
        uint64_t maxCount = (1 << 10) - 1;
        msg.setLen(maxLen);
        msg.setOffset(maxOffset);
        msg.setMsgCount(maxCount);
        EXPECT_EQ(maxLen, msg.getLen());
        EXPECT_EQ(maxOffset, msg.getOffset());
        EXPECT_EQ(maxCount, msg.getMsgCount());
    }
}

}; // namespace common
}; // namespace swift
