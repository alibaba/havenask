#include "indexlib/index/normal/inverted_index/truncate/truncate_meta_trigger.h"
#include "indexlib/misc/exception.h"

using namespace std;
IE_NAMESPACE_USE(common);

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, TruncateMetaTrigger);

TruncateMetaTrigger::TruncateMetaTrigger(uint64_t truncThreshold) 
    : mTruncThreshold(truncThreshold)
{
    (void)mTruncThreshold;
}

TruncateMetaTrigger::~TruncateMetaTrigger() 
{
}

bool TruncateMetaTrigger::NeedTruncate(const TruncateTriggerInfo &info)
{
    if (!mMetaReader)
    {
        IE_LOG(WARN, "truncate_meta for some index does not "
                             "exist in full index, check your config!");
        return false;
    }
    return mMetaReader->IsExist(info.GetDictKey());
}

void TruncateMetaTrigger::SetTruncateMetaReader(
        const TruncateMetaReaderPtr& reader)
{
    mMetaReader = reader;
}

IE_NAMESPACE_END(index);

