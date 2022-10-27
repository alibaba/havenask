#ifndef __INDEXLIB_ATTRIBUTE_PATCH_MERGER_FACTORY_H
#define __INDEXLIB_ATTRIBUTE_PATCH_MERGER_FACTORY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_patch_merger.h"
#include "indexlib/config/attribute_config.h"

IE_NAMESPACE_BEGIN(index);

class AttributePatchMergerFactory
{
public:
    AttributePatchMergerFactory();
    ~AttributePatchMergerFactory();
public:
    static AttributePatchMerger* Create(
        const config::AttributeConfigPtr& attributeConfig,
        const SegmentUpdateBitmapPtr& segUpdateBitmap = SegmentUpdateBitmapPtr());

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributePatchMergerFactory);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTE_PATCH_MERGER_FACTORY_H
