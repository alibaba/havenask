#ifndef __INDEXLIB_DEFAULT_ATTRIBUTE_VALUE_INITIALIZER_CREATOR_H
#define __INDEXLIB_DEFAULT_ATTRIBUTE_VALUE_INITIALIZER_CREATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/field_format/attribute/attribute_value_initializer_creator.h"
#include "indexlib/common/field_format/attribute/default_attribute_value_initializer.h"
#include "indexlib/common/field_format/attribute/attribute_convertor.h"

DECLARE_REFERENCE_CLASS(config, FieldConfig);

IE_NAMESPACE_BEGIN(common);

class DefaultAttributeValueInitializerCreator : public AttributeValueInitializerCreator
{
public:
    DefaultAttributeValueInitializerCreator(
            const config::FieldConfigPtr& fieldConfig);

    ~DefaultAttributeValueInitializerCreator();

public:
    // for reader (global data)
    AttributeValueInitializerPtr Create(
            const index_base::PartitionDataPtr& partitionData) override;

    // for segment build (local segment data)
    AttributeValueInitializerPtr Create(
            const index::InMemorySegmentReaderPtr& inMemSegmentReader) override;

public:
    DefaultAttributeValueInitializerPtr Create();
    
private:
    config::FieldConfigPtr mFieldConfig;
    AttributeConvertorPtr mConvertor;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DefaultAttributeValueInitializerCreator);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_DEFAULT_ATTRIBUTE_VALUE_INITIALIZER_CREATOR_H
