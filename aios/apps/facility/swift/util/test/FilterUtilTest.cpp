#include "swift/util/FilterUtil.h"

#include <stddef.h>
#include <stdint.h>

#include "swift/common/Common.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "unittest/unittest.h"

namespace swift {
namespace util {

using namespace common;

class FilterUtilTest : public TESTBASE {};

TEST_F(FilterUtilTest, testCompressPayload) {
    { // 0-65535
        swift::protocol::PartitionId partId;
        partId.set_from(0);
        partId.set_to(65535);
        ASSERT_EQ(0b1, FilterUtil::compressPayload(partId, 0));
        ASSERT_EQ(0b1, FilterUtil::compressPayload(partId, 1023));
        ASSERT_EQ(0b10, FilterUtil::compressPayload(partId, 1024));
        ASSERT_EQ(0x8000000000000000, FilterUtil::compressPayload(partId, 65535));
        ASSERT_EQ(0b0, FilterUtil::compressPayload(partId, 65536));
    }
    { // 0-63
        swift::protocol::PartitionId partId;
        partId.set_from(0);
        partId.set_to(63);
        ASSERT_EQ(0b1, FilterUtil::compressPayload(partId, 0));
        ASSERT_EQ(0b10, FilterUtil::compressPayload(partId, 1));
        ASSERT_EQ(0b1000, FilterUtil::compressPayload(partId, 3));
        uint64_t result = 1ul << 63;
        ASSERT_EQ(result, FilterUtil::compressPayload(partId, 63));
    }
    { // 64-127
        swift::protocol::PartitionId partId;
        partId.set_from(64);
        partId.set_to(127);
        ASSERT_EQ(0x8000000000000000, FilterUtil::compressPayload(partId, 127));
    }
    { // 0-1023
        swift::protocol::PartitionId partId;
        partId.set_from(0);
        partId.set_to(1023);
        ASSERT_EQ(0b1, FilterUtil::compressPayload(partId, 0));
        ASSERT_EQ(0b1, FilterUtil::compressPayload(partId, 1));
        ASSERT_EQ(0b1, FilterUtil::compressPayload(partId, 2));
        ASSERT_EQ(0b1, FilterUtil::compressPayload(partId, 15));
        ASSERT_EQ(0b10, FilterUtil::compressPayload(partId, 16));
        ASSERT_EQ(0x8000000000000000, FilterUtil::compressPayload(partId, 1023));
        ASSERT_EQ(0b0, FilterUtil::compressPayload(partId, 1024));
    }
    { // 0-1024
        swift::protocol::PartitionId partId;
        partId.set_from(0);
        partId.set_to(1024);
        ASSERT_EQ(0b1, FilterUtil::compressPayload(partId, 0));
        ASSERT_EQ(0b1, FilterUtil::compressPayload(partId, 15));
        ASSERT_EQ(0b1, FilterUtil::compressPayload(partId, 16));
        ASSERT_EQ(0b10, FilterUtil::compressPayload(partId, 17));
        ASSERT_EQ(0b10, FilterUtil::compressPayload(partId, 33));
        ASSERT_EQ(0b100, FilterUtil::compressPayload(partId, 34));
        ASSERT_EQ(0x1000000000000000, FilterUtil::compressPayload(partId, 1023));
        ASSERT_EQ(0x1000000000000000, FilterUtil::compressPayload(partId, 1024));
        ASSERT_EQ(0b0, FilterUtil::compressPayload(partId, 1025));
    }
    { // 1-1024
        swift::protocol::PartitionId partId;
        partId.set_from(1);
        partId.set_to(1024);
        ASSERT_EQ(0b0, FilterUtil::compressPayload(partId, 0));
        ASSERT_EQ(0b1, FilterUtil::compressPayload(partId, 1));
        ASSERT_EQ(0b1, FilterUtil::compressPayload(partId, 2));
        ASSERT_EQ(0b1, FilterUtil::compressPayload(partId, 15));
        ASSERT_EQ(0b1, FilterUtil::compressPayload(partId, 16));
        ASSERT_EQ(0b10, FilterUtil::compressPayload(partId, 17));
        ASSERT_EQ(0x8000000000000000, FilterUtil::compressPayload(partId, 1024));
        ASSERT_EQ(0b0, FilterUtil::compressPayload(partId, 1025));
    }
    { // 1024-2047
        swift::protocol::PartitionId partId;
        partId.set_from(1024);
        partId.set_to(2047);
        ASSERT_EQ(0b0, FilterUtil::compressPayload(partId, 0));
        ASSERT_EQ(0b0, FilterUtil::compressPayload(partId, 1023));
        ASSERT_EQ(0b1, FilterUtil::compressPayload(partId, 1024));
        ASSERT_EQ(0x8000000000000000, FilterUtil::compressPayload(partId, 2047));
    }
    { // 12345-12350
        swift::protocol::PartitionId partId;
        partId.set_from(12345);
        partId.set_to(12350);
        ASSERT_EQ(0b1, FilterUtil::compressPayload(partId, 12345));
        ASSERT_EQ(0b100000, FilterUtil::compressPayload(partId, 12350));
        ASSERT_EQ(0b0, FilterUtil::compressPayload(partId, 12351));
    }
}

TEST_F(FilterUtilTest, testCompressFilterRegion) {
    { // part 0-65535 precision 1024
        swift::protocol::PartitionId partId;
        partId.set_from(0);
        partId.set_to(65535);
        { // filter 0-65535
            swift::protocol::Filter filter;
            filter.set_from(0);
            filter.set_to(65535);
            ASSERT_EQ(UINT64_FULL_SET, FilterUtil::compressFilterRegion(partId, filter));
        }
        { // filter 0-1023
            swift::protocol::Filter filter;
            filter.set_from(0);
            filter.set_to(1023);
            ASSERT_EQ(0b1, FilterUtil::compressFilterRegion(partId, filter));
        }
        { // filter 1023-1024
            swift::protocol::Filter filter;
            filter.set_from(1023);
            filter.set_to(1024);
            ASSERT_EQ(0b11, FilterUtil::compressFilterRegion(partId, filter));
        }
    }
    { // part 0-1023 precision 16
        swift::protocol::PartitionId partId;
        partId.set_from(0);
        partId.set_to(1023);
        { // filter 0-1023
            swift::protocol::Filter filter;
            filter.set_from(0);
            filter.set_to(1023);
            ASSERT_EQ(UINT64_FULL_SET, FilterUtil::compressFilterRegion(partId, filter));
        }
        { // filter 0-15
            swift::protocol::Filter filter;
            filter.set_from(0);
            filter.set_to(15);
            ASSERT_EQ(0b1, FilterUtil::compressFilterRegion(partId, filter));
        }
        { // filter 0-16
            swift::protocol::Filter filter;
            filter.set_from(0);
            filter.set_to(16);
            ASSERT_EQ(0b11, FilterUtil::compressFilterRegion(partId, filter));
        }
        { // filter 0-31
            swift::protocol::Filter filter;
            filter.set_from(0);
            filter.set_to(31);
            ASSERT_EQ(0b11, FilterUtil::compressFilterRegion(partId, filter));
        }
        { // filter 0-32
            swift::protocol::Filter filter;
            filter.set_from(0);
            filter.set_to(32);
            ASSERT_EQ(0b111, FilterUtil::compressFilterRegion(partId, filter));
        }
    }
    { // part 1024-2047 precision 16
        swift::protocol::PartitionId partId;
        partId.set_from(1024);
        partId.set_to(2047);
        { // filter 0-1023
            swift::protocol::Filter filter;
            filter.set_from(0);
            filter.set_to(1023);
            ASSERT_EQ(UINT64_FULL_SET, FilterUtil::compressFilterRegion(partId, filter));
        }
        { // filter 0-15
            swift::protocol::Filter filter;
            filter.set_from(0);
            filter.set_to(15);
            ASSERT_EQ(0b1, FilterUtil::compressFilterRegion(partId, filter));
        }
        { // filter 0-16
            swift::protocol::Filter filter;
            filter.set_from(0);
            filter.set_to(16);
            ASSERT_EQ(0b11, FilterUtil::compressFilterRegion(partId, filter));
        }
        { // filter 0-31
            swift::protocol::Filter filter;
            filter.set_from(0);
            filter.set_to(31);
            ASSERT_EQ(0b11, FilterUtil::compressFilterRegion(partId, filter));
        }
        { // filter 0-32
            swift::protocol::Filter filter;
            filter.set_from(0);
            filter.set_to(32);
            ASSERT_EQ(0b111, FilterUtil::compressFilterRegion(partId, filter));
        }
    }
}

TEST_F(FilterUtilTest, testSimple) {
    {
        // 0-65535 precision 1024
        swift::protocol::PartitionId partId;
        partId.set_from(0);
        partId.set_to(65535);
        { // hit 0-1023
            swift::protocol::Filter filter;
            filter.set_from(0);
            filter.set_to(37);
            ASSERT_TRUE(FilterUtil::compressFilterRegion(partId, filter) & FilterUtil::compressPayload(partId, 0));
            ASSERT_TRUE(FilterUtil::compressFilterRegion(partId, filter) & FilterUtil::compressPayload(partId, 37));
            ASSERT_TRUE(FilterUtil::compressFilterRegion(partId, filter) & FilterUtil::compressPayload(partId, 1023));
            ASSERT_FALSE(FilterUtil::compressFilterRegion(partId, filter) & FilterUtil::compressPayload(partId, 1024));
        }
        { // hit 0-2047
            swift::protocol::Filter filter;
            filter.set_from(0);
            filter.set_to(1024);
            ASSERT_TRUE(FilterUtil::compressFilterRegion(partId, filter) & FilterUtil::compressPayload(partId, 0));
            ASSERT_TRUE(FilterUtil::compressFilterRegion(partId, filter) & FilterUtil::compressPayload(partId, 1024));
            ASSERT_TRUE(FilterUtil::compressFilterRegion(partId, filter) & FilterUtil::compressPayload(partId, 2047));
            ASSERT_FALSE(FilterUtil::compressFilterRegion(partId, filter) & FilterUtil::compressPayload(partId, 2048));
        }
        { // hit 0-3071
            swift::protocol::Filter filter;
            filter.set_from(1022);
            filter.set_to(2048);
            ASSERT_TRUE(FilterUtil::compressFilterRegion(partId, filter) & FilterUtil::compressPayload(partId, 1022));
            ASSERT_TRUE(FilterUtil::compressFilterRegion(partId, filter) & FilterUtil::compressPayload(partId, 2048));
            ASSERT_FALSE(FilterUtil::compressFilterRegion(partId, filter) & FilterUtil::compressPayload(partId, 3072));
        }
    }
    {
        // 21845-43690 precision 342
        swift::protocol::PartitionId partId;
        partId.set_from(21845);
        partId.set_to(43690);
        { // hit 16384-32768
            swift::protocol::Filter filter;
            filter.set_from(16384);
            filter.set_to(32768);
            ASSERT_TRUE(FilterUtil::compressFilterRegion(partId, filter) & FilterUtil::compressPayload(partId, 21845));
            ASSERT_TRUE(FilterUtil::compressFilterRegion(partId, filter) & FilterUtil::compressPayload(partId, 21846));
            ASSERT_TRUE(FilterUtil::compressFilterRegion(partId, filter) & FilterUtil::compressPayload(partId, 32768));
            ASSERT_TRUE(FilterUtil::compressFilterRegion(partId, filter) & FilterUtil::compressPayload(partId, 32769));
            ASSERT_TRUE(FilterUtil::compressFilterRegion(partId, filter) & FilterUtil::compressPayload(partId, 32788));
            ASSERT_FALSE(FilterUtil::compressFilterRegion(partId, filter) & FilterUtil::compressPayload(partId, 32789));
        }
    }
    {
        // 0-63 precision 1
        swift::protocol::PartitionId partId;
        partId.set_from(0);
        partId.set_to(63);
        { // hit 0-1
            swift::protocol::Filter filter;
            filter.set_from(0);
            filter.set_to(1);
            ASSERT_TRUE(FilterUtil::compressFilterRegion(partId, filter) & FilterUtil::compressPayload(partId, 0));
            ASSERT_TRUE(FilterUtil::compressFilterRegion(partId, filter) & FilterUtil::compressPayload(partId, 1));
            ASSERT_FALSE(FilterUtil::compressFilterRegion(partId, filter) & FilterUtil::compressPayload(partId, 2));
        }
        { // hit 4-17
            swift::protocol::Filter filter;
            filter.set_from(4);
            filter.set_to(17);
            ASSERT_TRUE(FilterUtil::compressFilterRegion(partId, filter) & FilterUtil::compressPayload(partId, 4));
            ASSERT_TRUE(FilterUtil::compressFilterRegion(partId, filter) & FilterUtil::compressPayload(partId, 17));
            ASSERT_FALSE(FilterUtil::compressFilterRegion(partId, filter) & FilterUtil::compressPayload(partId, 18));
            ASSERT_FALSE(FilterUtil::compressFilterRegion(partId, filter) & FilterUtil::compressPayload(partId, 19));
        }
        { // hit 30-60
            swift::protocol::Filter filter;
            filter.set_from(30);
            filter.set_to(60);
            ASSERT_TRUE(FilterUtil::compressFilterRegion(partId, filter) & FilterUtil::compressPayload(partId, 30));
            ASSERT_TRUE(FilterUtil::compressFilterRegion(partId, filter) & FilterUtil::compressPayload(partId, 60));
            ASSERT_FALSE(FilterUtil::compressFilterRegion(partId, filter) & FilterUtil::compressPayload(partId, 61));
            ASSERT_FALSE(FilterUtil::compressFilterRegion(partId, filter) & FilterUtil::compressPayload(partId, 63));
            ASSERT_FALSE(FilterUtil::compressFilterRegion(partId, filter) & FilterUtil::compressPayload(partId, 67));
        }
        { // hit nothing
            swift::protocol::Filter filter;
            filter.set_from(30);
            filter.set_to(20);
            ASSERT_FALSE(FilterUtil::compressFilterRegion(partId, filter) & FilterUtil::compressPayload(partId, 30));
        }
    }
    {
        // 64-127 precision 1
        swift::protocol::PartitionId partId;
        partId.set_from(64);
        partId.set_to(127);
        { // hit 64-127
            swift::protocol::Filter filter;
            filter.set_from(64);
            filter.set_to(127);
            ASSERT_FALSE(FilterUtil::compressFilterRegion(partId, filter) & FilterUtil::compressPayload(partId, 2));
            ASSERT_TRUE(FilterUtil::compressFilterRegion(partId, filter) & FilterUtil::compressPayload(partId, 64));
            ASSERT_TRUE(FilterUtil::compressFilterRegion(partId, filter) & FilterUtil::compressPayload(partId, 127));
            ASSERT_FALSE(FilterUtil::compressFilterRegion(partId, filter) & FilterUtil::compressPayload(partId, 128));
        }
    }

    {
        // default filter
        swift::protocol::ConsumptionRequest request;
        // 64-127 precision 1
        swift::protocol::PartitionId partId;
        partId.set_from(64);
        partId.set_to(127);
        {
            ASSERT_FALSE(FilterUtil::compressFilterRegion(partId, request.filter()) &
                         FilterUtil::compressPayload(partId, 2));
            ASSERT_TRUE(FilterUtil::compressFilterRegion(partId, request.filter()) &
                        FilterUtil::compressPayload(partId, 64));
            ASSERT_TRUE(FilterUtil::compressFilterRegion(partId, request.filter()) &
                        FilterUtil::compressPayload(partId, 127));
            ASSERT_FALSE(FilterUtil::compressFilterRegion(partId, request.filter()) &
                         FilterUtil::compressPayload(partId, 128));
        }
    }
}

TEST_F(FilterUtilTest, testPrimeSample) {
    swift::protocol::PartitionId partId;
    swift::protocol::Filter filter;
    for (size_t from = 0; from < 65535; from = from + 5751) {
        partId.set_from(from);
        for (size_t to = from; to < 65535; to = to + 5751) {
            partId.set_to(to);
            for (size_t filterFrom = from; filterFrom <= to; filterFrom = filterFrom + 6997) {
                filter.set_from(filterFrom);
                for (size_t filterTo = filterFrom; filterTo <= to; filterTo = filterTo + 6997) {
                    filter.set_to(filterTo);
                    for (size_t payload = filterFrom; payload <= filterTo; payload = payload + 1153) {
                        if (payload <= to && payload >= from) {
                            ASSERT_TRUE(FilterUtil::compressFilterRegion(partId, filter) &
                                        FilterUtil::compressPayload(partId, payload));
                        }
                    }
                }
            }
        }
    }
}

} // namespace util
} // namespace swift
