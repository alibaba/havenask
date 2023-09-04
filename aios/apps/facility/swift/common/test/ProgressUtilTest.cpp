#include "swift/common/ProgressUtil.h"

#include <iosfwd>
#include <stdint.h>

#include "swift/protocol/SwiftMessage.pb.h"
#include "unittest/unittest.h"

using namespace std;
using namespace swift::protocol;

namespace swift {
namespace common {

class ProgressUtilTest : public TESTBASE {};

TEST_F(ProgressUtilTest, testUpdateProgress) {
    /*    ProgressUtil pru;
    ReaderProgress rp;

    // not set topic
    pru.updateProgress(rp);
    ASSERT_EQ(0, pru._range.size());

    // one singleProgress invalid, from should not greater than to
    SingleReaderProgress *srp = rp.add_progress();
    srp->set_topic("topic");
    srp->mutable_filter()->set_from(100);
    srp->mutable_filter()->set_to(50);
    pru.updateProgress(rp);
    ASSERT_EQ(0, pru._range.size());

    // normal
    srp->mutable_filter()->set_to(200);
    srp->set_timestamp(100);

    srp = rp.add_progress();
    srp->set_topic("topic");
    srp->mutable_filter()->set_from(300);
    srp->mutable_filter()->set_to(400);
    srp->set_timestamp(300);

    srp = rp.add_progress();
    srp->set_topic("topic2");
    srp->mutable_filter()->set_from(100);
    srp->mutable_filter()->set_to(200);
    srp->set_timestamp(50);

    pru.updateProgress(rp);
    ASSERT_EQ(2, pru._range.size());
    ProgressUtil::KeyType key1("topic", 0, 0);
    auto iter = pru._range.find(key1);
    ASSERT_TRUE(iter != pru._range.end());
    for (uint32_t idx = 0; idx < MAX_RANGE_COUNT; ++idx) {
        if (idx >= 100 && idx <= 200) {
            ASSERT_EQ(100, iter->second[idx]);
        } else if (idx >= 300 && idx <= 400) {
            ASSERT_EQ(300, iter->second[idx]);
        } else {
            ASSERT_EQ(0, iter->second[idx]);
        }
    }

    ProgressUtil::KeyType key2("topic2", 0, 0);
    iter = pru._range.find(key2);
    ASSERT_TRUE(iter != pru._range.end());
    for (uint32_t idx = 0; idx < MAX_RANGE_COUNT; ++idx) {
        if (idx >= 100 && idx <= 200) {
            ASSERT_EQ(50, iter->second[idx]);
        } else {
            ASSERT_EQ(0, iter->second[idx]);
        }
    }

    // update
    SingleReaderProgress *srp1 = rp.mutable_progress(0);
    srp1->set_timestamp(50);
    SingleReaderProgress *srp2 = rp.mutable_progress(1);
    srp2->mutable_filter()->set_from(250);
    srp2->mutable_filter()->set_to(350);
    srp2->set_timestamp(350);
    SingleReaderProgress *srp3 = rp.mutable_progress(2);
    srp3->set_timestamp(200);
    pru.updateProgress(rp);
    iter = pru._range.find(key1);
    ASSERT_TRUE(iter != pru._range.end());
    for (uint32_t idx = 0; idx < MAX_RANGE_COUNT; ++idx) {
        if (idx >= 100 && idx <= 200) {
            ASSERT_EQ(50, iter->second[idx]);
        } else if (idx >= 250 && idx <= 350) {
            ASSERT_EQ(350, iter->second[idx]);
        } else if (idx > 350 && idx <= 400) {
            ASSERT_EQ(300, iter->second[idx]);
        } else {
            ASSERT_EQ(0, iter->second[idx]);
        }
    }
    iter = pru._range.find(key2);
    ASSERT_TRUE(iter != pru._range.end());
    for (uint32_t idx = 0; idx < MAX_RANGE_COUNT; ++idx) {
        if (idx >= 100 && idx <= 200) {
            ASSERT_EQ(200, iter->second[idx]);
        } else {
            ASSERT_EQ(0, iter->second[idx]);
        }
    }

    // test constructor update
    ProgressUtil pruCtr(rp);
    iter = pruCtr._range.find(key1);
    ASSERT_TRUE(iter != pruCtr._range.end());
    for (uint32_t idx = 0; idx < MAX_RANGE_COUNT; ++idx) {
        if (idx >= 100 && idx <= 200) {
            ASSERT_EQ(50, iter->second[idx]);
        } else if (idx >= 250 && idx <= 350) {
            ASSERT_EQ(350, iter->second[idx]);
        } else {
            ASSERT_EQ(0, iter->second[idx]);
        }
    }
    iter = pruCtr._range.find(key2);
    ASSERT_TRUE(iter != pruCtr._range.end());
    for (uint32_t idx = 0; idx < MAX_RANGE_COUNT; ++idx) {
        if (idx >= 100 && idx <= 200) {
            ASSERT_EQ(200, iter->second[idx]);
        } else {
            ASSERT_EQ(0, iter->second[idx]);
        }
    }
    */
}

TEST_F(ProgressUtilTest, testGetCoverMinTimestamp) {
    ReaderProgress progress;
    TopicReaderProgress *topicProgress = progress.add_topicprogress();
    topicProgress->set_topicname("topic");
    topicProgress->set_uint8filtermask(10);
    topicProgress->set_uint8maskresult(10);
    PartReaderProgress *partProgress = topicProgress->add_partprogress();
    partProgress->set_from(100);
    partProgress->set_to(200);
    partProgress->set_timestamp(100);

    partProgress = topicProgress->add_partprogress();
    partProgress->set_from(300);
    partProgress->set_to(400);
    partProgress->set_timestamp(300);
    ProgressUtil progressUtil;
    ASSERT_TRUE(progressUtil.updateProgress(*topicProgress));

    int64_t timestamp = progressUtil.getCoverMinTimestamp(10000000, 200);
    ASSERT_EQ(0, timestamp);
    timestamp = progressUtil.getCoverMinTimestamp(100, 200);
    ASSERT_EQ(100, timestamp);
    timestamp = progressUtil.getCoverMinTimestamp(101, 199);
    ASSERT_EQ(100, timestamp);
    timestamp = progressUtil.getCoverMinTimestamp(99, 200);
    ASSERT_EQ(0, timestamp);
    timestamp = progressUtil.getCoverMinTimestamp(250, 350);
    ASSERT_EQ(0, timestamp);
}

} // namespace common
} // namespace swift
