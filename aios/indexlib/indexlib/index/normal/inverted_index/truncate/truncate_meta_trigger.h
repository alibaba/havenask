#ifndef __INDEXLIB_TRUNCATE_META_TRIGGER_H
#define __INDEXLIB_TRUNCATE_META_TRIGGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_trigger.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_meta_reader.h"

IE_NAMESPACE_BEGIN(index);

class TruncateMetaTrigger : public TruncateTrigger
{
public:
    TruncateMetaTrigger(uint64_t truncThreshold);
    ~TruncateMetaTrigger();

public:
    bool NeedTruncate(const TruncateTriggerInfo &info);
    void SetTruncateMetaReader(const TruncateMetaReaderPtr& reader);

private:
    uint64_t mTruncThreshold;
    TruncateMetaReaderPtr mMetaReader;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TruncateMetaTrigger);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_TRUNCATE_META_TRIGGER_H
