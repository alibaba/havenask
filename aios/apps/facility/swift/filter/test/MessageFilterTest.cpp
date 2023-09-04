#include "swift/filter/MessageFilter.h"

#include <stdint.h>

#include "swift/common/Common.h"
#include "swift/common/FileMessageMeta.h"
#include "swift/common/MemoryMessage.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "unittest/unittest.h"

using namespace swift::protocol;
using namespace swift::common;
namespace swift {
namespace filter {

class MessageFilterTest : public TESTBASE {
private:
    void innerTestFilter(const MessageFilter &filter, bool merged, uint16_t payload, uint8_t maskPayload, bool result);
};

TEST_F(MessageFilterTest, testRangeFilter) {
#define TEST_RANGE_HELPER(from, to, payload, result)                                                                   \
    {                                                                                                                  \
        Filter filter;                                                                                                 \
        filter.set_from(from);                                                                                         \
        filter.set_to(to);                                                                                             \
        MessageFilter msgFilter(filter);                                                                               \
        EXPECT_TRUE(msgFilter.rangeFilter(payload) == result);                                                         \
    }

    TEST_RANGE_HELPER(0, 100, 10, true);
    TEST_RANGE_HELPER(0, 100, 0, true);
    TEST_RANGE_HELPER(0, 100, 100, true);
    TEST_RANGE_HELPER(0, 100, 1000, false);
#undef TEST_RANGE_HELPER
}

TEST_F(MessageFilterTest, testMaskFilter) {
#define TEST_MASK_HELPER(mask, maskResult, msgMaskPayload, result)                                                     \
    {                                                                                                                  \
        Filter filter;                                                                                                 \
        filter.set_uint8filtermask(mask);                                                                              \
        filter.set_uint8maskresult(maskResult);                                                                        \
        MessageFilter msgFilter(filter);                                                                               \
        EXPECT_TRUE(msgFilter.maskFilter(msgMaskPayload) == result);                                                   \
    }

    TEST_MASK_HELPER(0x0f, 0x08, 0x08, true);
    TEST_MASK_HELPER(0x0f, 0x04, 0x08, false);
    TEST_MASK_HELPER(0x00, 0x00, 0x08, true);
    TEST_MASK_HELPER(0x00, 0x08, 0x08, false);

#undef TEST_MASK_HELPER
}

TEST_F(MessageFilterTest, testFilter) {
#define TEST_FILTER_HELPER(from, to, mergedTo, payload, mask, maskResult, maskPayload, result, merged)                 \
    {                                                                                                                  \
        Filter filter;                                                                                                 \
        filter.set_from(from);                                                                                         \
        filter.set_to(to);                                                                                             \
        filter.set_mergedto(mergedTo);                                                                                 \
        filter.set_uint8filtermask(mask);                                                                              \
        filter.set_uint8maskresult(maskResult);                                                                        \
        MessageFilter msgFilter(filter);                                                                               \
        innerTestFilter(msgFilter, merged, payload, maskPayload, result);                                              \
    }

    TEST_FILTER_HELPER(0, 100, 100, 10, 0x0f, 0x08, 0x08, true, false);
    TEST_FILTER_HELPER(0, 100, 100, 1000, 0x0f, 0x08, 0x08, false, false);
    TEST_FILTER_HELPER(0, 100, 100, 10, 0x0f, 0x08, 0x04, false, false);
    TEST_FILTER_HELPER(0, 100, 100, 1000, 0x0f, 0x08, 0x04, false, false);
    TEST_FILTER_HELPER(0, 100, 1000, 1000, 0x0f, 0x08, 0x08, true, true);
    TEST_FILTER_HELPER(0, 100, 1000, 1000, 0x0f, 0x08, 0x08, false, false);

#undef TEST_FILTER_HELPER
}

void MessageFilterTest::innerTestFilter(
    const MessageFilter &filter, bool merged, uint16_t payload, uint8_t maskPayload, bool result) {
    Message msg;
    msg.set_merged(merged);
    msg.set_uint16payload(payload);
    msg.set_uint8maskpayload(maskPayload);
    EXPECT_TRUE(result == filter.filter(msg));

    MemoryMessage memMessage;
    memMessage.setMerged(merged);
    memMessage.setPayload(payload);
    memMessage.setMaskPayload(maskPayload);
    EXPECT_TRUE(result == filter.filter(memMessage));

    FileMessageMeta meta;
    meta.isMerged = merged;
    meta.payload = payload;
    meta.maskPayload = maskPayload;
    EXPECT_TRUE(result == filter.filter(meta));
}

} // namespace filter
} // namespace swift
