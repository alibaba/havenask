#include "indexlib/index/normal/inverted_index/truncate/default_truncate_trigger.h"

using namespace std;
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, DefaultTruncateTrigger);

DefaultTruncateTrigger::DefaultTruncateTrigger(uint64_t truncThreshold) 
{
    mTruncThreshold = truncThreshold;
}

DefaultTruncateTrigger::~DefaultTruncateTrigger() 
{
}

bool DefaultTruncateTrigger::NeedTruncate(const TruncateTriggerInfo &info)
{
    return info.GetDF() > (df_t)mTruncThreshold;
}

IE_NAMESPACE_END(index);

