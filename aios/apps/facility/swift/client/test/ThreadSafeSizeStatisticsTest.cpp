#include "swift/client/ThreadSafeSizeStatistics.h"

#include <stdint.h>

#include "unittest/unittest.h"

namespace swift {
namespace client {

class ThreadSafeSizeStatisticsTest : public TESTBASE {};

TEST_F(ThreadSafeSizeStatisticsTest, testSimpleProcess) {
    ThreadSafeSizeStatistics sizeStatistics;

    EXPECT_EQ((int64_t)0, sizeStatistics.getSize());
    sizeStatistics.addSize((int64_t)12);
    sizeStatistics.addSize((int64_t)20);
    EXPECT_EQ((int64_t)32, sizeStatistics.getSize());
    sizeStatistics.subSize((int64_t)16);
    EXPECT_EQ((int64_t)16, sizeStatistics.getSize());
    sizeStatistics.subSize((int64_t)20);
    EXPECT_EQ((int64_t)0, sizeStatistics.getSize());
}

} // namespace client
} // namespace swift
