#ifndef __INDEXLIB_ATTRIBUTE_VALUE_INITIALIZER_CREATOR_H
#define __INDEXLIB_ATTRIBUTE_VALUE_INITIALIZER_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/field_format/attribute/attribute_value_initializer.h"

DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index, InMemorySegmentReader);

IE_NAMESPACE_BEGIN(common);

class AttributeValueInitializerCreator
{
public:
    AttributeValueInitializerCreator() {}
    virtual ~AttributeValueInitializerCreator() {}

public:
    // for reader (global data)
    virtual AttributeValueInitializerPtr Create(
            const index_base::PartitionDataPtr& partitionData) = 0;

    // for segment build (local segment data)
    virtual AttributeValueInitializerPtr Create(
            const index::InMemorySegmentReaderPtr& inMemSegmentReader) = 0;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributeValueInitializerCreator);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_ATTRIBUTE_VALUE_INITIALIZER_CREATOR_H
