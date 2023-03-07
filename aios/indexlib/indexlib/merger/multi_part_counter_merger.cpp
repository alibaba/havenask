#include "indexlib/merger/multi_part_counter_merger.h"
#include "indexlib/misc/exception.h"
#include "indexlib/util/counter/counter_base.h"
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/util/counter/state_counter.h"

using namespace std;

IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, MultiPartCounterMerger);

MultiPartCounterMerger::MultiPartCounterMerger(bool hasSubPartition)
    : mCounterMap(new CounterMap)
    , mHasSub(hasSubPartition)
    , mDocCount(0)
    , mDeletedDocCount(0)
    , mSubDocCount(0)
    , mSubDeletedDocCount(0)
{
}

MultiPartCounterMerger::~MultiPartCounterMerger()
{
}

StateCounterPtr MultiPartCounterMerger::GetStateCounter(
        const string& counterName,
        bool isSub)
{
    string prefix = isSub ? "offline.sub_" : "offline.";
    string counterPath = prefix + counterName;
    auto stateCounter = mCounterMap->GetStateCounter(counterPath);
    if (!stateCounter)
    {
        INDEXLIB_FATAL_ERROR(InconsistentState, "init counter[%s] failed", counterPath.c_str());
    }
    return stateCounter;
}

void MultiPartCounterMerger::Merge(const string& counterMapContent) {
    mCounterMap->Merge(counterMapContent, CounterBase::FJT_MERGE);

    StateCounterPtr partitionDocCounter = GetStateCounter("partitionDocCount", false);
    StateCounterPtr deletedDocCounter = GetStateCounter("deletedDocCount", false);
    if (partitionDocCounter) {
        mDocCount += partitionDocCounter->Get();
        partitionDocCounter->Set(mDocCount);
    }
    if (deletedDocCounter) {
        mDeletedDocCount += deletedDocCounter->Get();
        deletedDocCounter->Set(mDeletedDocCount);
    }

    if (mHasSub) {
        StateCounterPtr subPartitionDocCounter = GetStateCounter("partitionDocCount", true);
        StateCounterPtr subDeletedDocCounter = GetStateCounter("deletedDocCount", true);
        if (subPartitionDocCounter) {
            mSubDocCount += subPartitionDocCounter->Get();
            subPartitionDocCounter->Set(mSubDocCount);
        }
        if (subDeletedDocCounter) {
            mSubDeletedDocCount += subDeletedDocCounter->Get();
            subDeletedDocCounter->Set(mSubDeletedDocCount);
        }
    }
}

string MultiPartCounterMerger::ToJsonString() {
    return mCounterMap->ToJsonString();
}

IE_NAMESPACE_END(merger);
