#ifndef __INDEXLIB_TRUNCATE_TRIGGER_H
#define __INDEXLIB_TRUNCATE_TRIGGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/term_meta.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_meta_reader.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_common.h"

IE_NAMESPACE_BEGIN(index);

class TruncateTrigger
{
public:
    TruncateTrigger() {}
    virtual ~TruncateTrigger() {}

public:
    virtual bool NeedTruncate(const TruncateTriggerInfo &info) = 0;
    virtual void SetTruncateMetaReader(const TruncateMetaReaderPtr &reader) {}

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TruncateTrigger);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_TRUNCATE_TRIGGER_H
