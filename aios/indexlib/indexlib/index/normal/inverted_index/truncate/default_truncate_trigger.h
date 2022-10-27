#ifndef __INDEXLIB_DEFAULT_TRUNCATE_TRIGGER_H
#define __INDEXLIB_DEFAULT_TRUNCATE_TRIGGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_trigger.h"

IE_NAMESPACE_BEGIN(index);

class DefaultTruncateTrigger : public TruncateTrigger
{
public:
    DefaultTruncateTrigger(uint64_t truncThreshold);
    ~DefaultTruncateTrigger();

public:
    bool NeedTruncate(const TruncateTriggerInfo &info);

private:
    uint64_t mTruncThreshold;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DefaultTruncateTrigger);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DEFAULT_TRUNCATE_TRIGGER_H
