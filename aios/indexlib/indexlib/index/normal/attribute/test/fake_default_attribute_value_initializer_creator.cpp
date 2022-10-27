#include "indexlib/common/field_format/attribute/default_attribute_value_initializer_creator.h"

using namespace std;
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, DefaultAttributeValueInitializerCreator);

DefaultAttributeValueInitializerCreator::DefaultAttributeValueInitializerCreator(
        const FieldConfigPtr& fieldConfig)
{
    assert(false);
}

DefaultAttributeValueInitializerCreator::~DefaultAttributeValueInitializerCreator() 
{
}

AttributeValueInitializerPtr DefaultAttributeValueInitializerCreator::Create(
        const index_base::PartitionDataPtr& partitionData)
{
    assert(false);
    return AttributeValueInitializerPtr();
}

AttributeValueInitializerPtr DefaultAttributeValueInitializerCreator::Create(
        const InMemorySegmentReaderPtr& inMemSegmentReader)
{
    assert(false);
    return AttributeValueInitializerPtr();
}

DefaultAttributeValueInitializerPtr DefaultAttributeValueInitializerCreator::Create()
{
    assert(false);
    return DefaultAttributeValueInitializerPtr();
}

IE_NAMESPACE_END(index);

