#pragma once

#include <memory>
#include <unistd.h>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/segment/dump_segment_container.h"

namespace indexlib { namespace test {

class SlowDumpSegmentContainer : public partition::DumpSegmentContainer
{
public:
    SlowDumpSegmentContainer(int64_t timeToSleepInMiniSecond, bool slowClearDumpSegment = true,
                             bool slowGetOneSegmentToDump = true)
        : mTimeToSleep(timeToSleepInMiniSecond)
        , mSleep(true)
        , mSlowClearDumpSegment(slowClearDumpSegment)
        , mSlowGetOneSegmentToDump(slowGetOneSegmentToDump)
    {
    }
    ~SlowDumpSegmentContainer() {}

public:
    void EnableSlowSleep() { mSleep = true; }
    void DisableSlowSleep() { mSleep = false; }

public:
    partition::NormalSegmentDumpItemPtr GetOneSegmentItemToDump() override
    {
        if (!mSlowGetOneSegmentToDump) {
            return partition::DumpSegmentContainer::GetOneSegmentItemToDump();
        }
        int64_t timeToSleep = mTimeToSleep;
        while (timeToSleep > 0) {
            if (!mSleep) {
                break;
            }
            usleep(1000);
            timeToSleep--;
        }
        return partition::DumpSegmentContainer::GetOneSegmentItemToDump();
    }

    partition::NormalSegmentDumpItemPtr GetOneValidSegmentItemToDump() const override
    {
        if (!mSlowGetOneSegmentToDump) {
            return partition::DumpSegmentContainer::GetOneValidSegmentItemToDump();
        }
        int64_t timeToSleep = mTimeToSleep;
        while (timeToSleep > 0) {
            if (!mSleep) {
                break;
            }
            usleep(1000);
            timeToSleep--;
        }
        return partition::DumpSegmentContainer::GetOneValidSegmentItemToDump();
    }

    void WaitEmpty()
    {
        while (GetDumpItemSize() > 0) {
            usleep(1000);
        }
    }

    void ClearDumpedSegment() override
    {
        if (!mSlowClearDumpSegment) {
            return partition::DumpSegmentContainer::ClearDumpedSegment();
        }
        int64_t timeToSleep = mTimeToSleep;
        while (timeToSleep > 0) {
            if (!mSleep) {
                break;
            }
            usleep(1000);
            timeToSleep--;
        }
        return partition::DumpSegmentContainer::ClearDumpedSegment();
    }

private:
    int64_t mTimeToSleep;
    volatile bool mSleep;
    bool mSlowClearDumpSegment;
    bool mSlowGetOneSegmentToDump;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SlowDumpSegmentContainer);
}} // namespace indexlib::test
