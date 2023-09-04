#include "swift/broker/storage/TimestampAllocator.h"

#include <iosfwd>
#include <stdint.h>
#include <unistd.h>

#include "autil/TimeUtility.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace swift {
namespace storage {

class TimestampAllocatorTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void TimestampAllocatorTest::setUp() {}

void TimestampAllocatorTest::tearDown() {}

TEST_F(TimestampAllocatorTest, testSimple) {
    {
        TimestampAllocator allocator;
        int64_t t1 = autil::TimeUtility::currentTime() + 1000000;
        int64_t t2 = allocator.getNextMsgTimestamp(t1);
        ASSERT_EQ(t1 + 1, t2);
    }
    {
        TimestampAllocator allocator;
        int64_t t1 = allocator.getCurrentTimestamp();
        usleep(10000);
        int64_t t2 = allocator.getCurrentTimestamp();
        ASSERT_TRUE(t1 < t2);
    }
    {
        TimestampAllocator allocator(10000000);
        int64_t t1 = allocator.getCurrentTimestamp();
        usleep(10000);
        int64_t t2 = allocator.getCurrentTimestamp();
        ASSERT_EQ(t1, t2);
        int64_t t3 = allocator.getNextMsgTimestamp(t2);
        ASSERT_EQ(t3, t2 + 1);
    }
    {
        TimestampAllocator allocator(-10000000);
        int64_t t1 = allocator.getCurrentTimestamp();
        usleep(10000);
        int64_t t2 = allocator.getCurrentTimestamp();
        ASSERT_TRUE(t1 < t2);
        usleep(10000);
        int64_t t3 = allocator.getNextMsgTimestamp(t2);
        ASSERT_TRUE(t3 > t2 + 1);
    }
}
}; // namespace storage
}; // namespace swift
