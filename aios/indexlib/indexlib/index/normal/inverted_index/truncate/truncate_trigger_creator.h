#ifndef __INDEXLIB_TRUNCATE_TRIGGER_CREATOR_H
#define __INDEXLIB_TRUNCATE_TRIGGER_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_common.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_trigger.h"

DECLARE_REFERENCE_CLASS(config, TruncateIndexProperty);

IE_NAMESPACE_BEGIN(index);

class TruncateTriggerCreator
{
public:
    TruncateTriggerCreator();
    ~TruncateTriggerCreator();

public:
    static TruncateTriggerPtr Create(
            const config::TruncateIndexProperty &truncateIndexProperty);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TruncateTriggerCreator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_TRUNCATE_TRIGGER_CREATOR_H
