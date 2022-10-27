#ifndef __INDEXLIB_ATTRIBUTE_SEGMENT_MODIFIER_H
#define __INDEXLIB_ATTRIBUTE_SEGMENT_MODIFIER_H

#include <tr1/memory>
#include <autil/ConstString.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/file_system/directory.h"

IE_NAMESPACE_BEGIN(index);

class AttributeSegmentModifier
{
public:
    AttributeSegmentModifier() {}
    virtual ~AttributeSegmentModifier() {}

public:
    virtual void Update(docid_t docId, attrid_t attrId, 
                        const autil::ConstString& attrValue) = 0;

    virtual void Dump(const file_system::DirectoryPtr& dir,
                      segmentid_t srcSegment) = 0;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributeSegmentModifier);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTE_SEGMENT_MODIFIER_H
