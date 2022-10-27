#include "indexlib/index/normal/attribute/accessor/attribute_patch_work_item.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, AttributePatchWorkItem);

void AttributePatchWorkItem::process()
{
    ScopedLock lock(mLock);
    IE_LOG(INFO, "load patch [%s] begin!", mIdentifier.c_str());
    int64_t startTime = autil::TimeUtility::currentTimeInMicroSeconds();
    size_t count = 0;
    while (HasNext() && count < mProcessLimit)
    {
        ProcessNext();
        count++;
    }
    int64_t endTime = autil::TimeUtility::currentTimeInMicroSeconds();
    mLastProcessTime = endTime - startTime;
    mLastProcessCount = count;
    IE_LOG(INFO, "load patch [%s] finished,  processTimeInMicroSeconds[%ld], processCount[%ld]",
           mIdentifier.c_str(), mLastProcessTime, mLastProcessCount);
}


IE_NAMESPACE_END(index);

