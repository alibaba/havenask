#ifndef __INDEXLIB_MULTI_PART_COUNTER_MERGER_H
#define __INDEXLIB_MULTI_PART_COUNTER_MERGER_H

#include <map>
#include <string>
#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(util, StateCounter);
DECLARE_REFERENCE_CLASS(util, CounterMap);

IE_NAMESPACE_BEGIN(merger);
class MultiPartCounterMerger {
public:
    explicit MultiPartCounterMerger(bool hasSubPartition);
    ~MultiPartCounterMerger();

    void Merge(const std::string& counterMapContent);
    std::string ToJsonString();

private:
    util::StateCounterPtr GetStateCounter(const std::string& counterName, bool isSub);

    util::CounterMapPtr mCounterMap;
    bool mHasSub;
    int64_t mDocCount;
    int64_t mDeletedDocCount;
    int64_t mSubDocCount;
    int64_t mSubDeletedDocCount;

    IE_LOG_DECLARE();

    friend class MultiPartCounterMergerTest;
};
IE_NAMESPACE_END(merger);

#endif // __INDEXLIB_MULTI_PART_COUNTER_MERGER_H
