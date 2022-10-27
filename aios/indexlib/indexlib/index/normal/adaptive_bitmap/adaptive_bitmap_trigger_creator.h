#ifndef __INDEXLIB_ADAPTIVE_BITMAP_TRIGGER_CREATOR_H
#define __INDEXLIB_ADAPTIVE_BITMAP_TRIGGER_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(index, ReclaimMap);
DECLARE_REFERENCE_CLASS(index, AdaptiveBitmapTrigger);
DECLARE_REFERENCE_CLASS(config, IndexConfig);

IE_NAMESPACE_BEGIN(index);

class AdaptiveBitmapTriggerCreator
{
public:
    AdaptiveBitmapTriggerCreator(const ReclaimMapPtr& reclaimMap);
    ~AdaptiveBitmapTriggerCreator();

public:
    AdaptiveBitmapTriggerPtr Create(
            const config::IndexConfigPtr& indexConf);

private:
    ReclaimMapPtr mReclaimMap;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AdaptiveBitmapTriggerCreator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ADAPTIVE_BITMAP_TRIGGER_CREATOR_H
