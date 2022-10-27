#ifndef __INDEXLIB_ATTRIBUTE_SEGMENT_PATCH_ITERATOR_CREATOR_H
#define __INDEXLIB_ATTRIBUTE_SEGMENT_PATCH_ITERATOR_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_patch_reader.h"
#include "indexlib/config/attribute_config.h"

IE_NAMESPACE_BEGIN(index);

class AttributeSegmentPatchIteratorCreator
{
public:
    AttributeSegmentPatchIteratorCreator();
    ~AttributeSegmentPatchIteratorCreator();

public:
    static AttributeSegmentPatchIterator* Create(
            const config::AttributeConfigPtr& attrConfig);

private:
    static AttributeSegmentPatchIterator* CreateVarNumSegmentPatchIterator(
            const config::AttributeConfigPtr& attrConfig);

    static AttributeSegmentPatchIterator* CreateSingleValueSegmentPatchIterator(
            const config::AttributeConfigPtr& attrConfig);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributeSegmentPatchIteratorCreator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTE_SEGMENT_PATCH_ITERATOR_CREATOR_H
