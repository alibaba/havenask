#include "indexlib/index/normal/attribute/accessor/attribute_segment_patch_iterator_creator.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_patch_reader.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_patch_reader.h"
#include "indexlib/config/field_type_traits.h"

using namespace std;
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, AttributeSegmentPatchIteratorCreator);

AttributeSegmentPatchIteratorCreator::AttributeSegmentPatchIteratorCreator() 
{
}

AttributeSegmentPatchIteratorCreator::~AttributeSegmentPatchIteratorCreator() 
{
}

AttributeSegmentPatchIterator* AttributeSegmentPatchIteratorCreator::Create(
        const AttributeConfigPtr& attrConfig)
{
    if (!attrConfig->IsAttributeUpdatable())
    {
        return NULL;
    }

    AttributeSegmentPatchIterator * iter = NULL;
    if (attrConfig->GetFieldType() == ft_string)
    {
        if (attrConfig->IsMultiValue())
        {
            return new VarNumAttributePatchReader<autil::MultiChar>(attrConfig);
        }
        return new VarNumAttributePatchReader<char>(attrConfig);
    }

    if (attrConfig->IsMultiValue())
    {
        iter = CreateVarNumSegmentPatchIterator(attrConfig);
    }
    else
    {
        iter = CreateSingleValueSegmentPatchIterator(attrConfig);
    }
    return iter;
}

AttributeSegmentPatchIterator* 
AttributeSegmentPatchIteratorCreator::CreateVarNumSegmentPatchIterator(
        const AttributeConfigPtr& attrConfig)
{
#define CREATE_VAR_NUM_ITER_BY_TYPE(field_type)                         \
    case field_type:                                                    \
    {                                                                   \
        typedef FieldTypeTraits<field_type>::AttrItemType Type;         \
        return new VarNumAttributePatchReader<Type>(attrConfig);        \
        break;                                                          \
    }                                                                   \

    switch(attrConfig->GetFieldType())
    {
        CREATE_VAR_NUM_ITER_BY_TYPE(ft_int32);
        CREATE_VAR_NUM_ITER_BY_TYPE(ft_uint32);
        CREATE_VAR_NUM_ITER_BY_TYPE(ft_float);
        CREATE_VAR_NUM_ITER_BY_TYPE(ft_fp8);
        CREATE_VAR_NUM_ITER_BY_TYPE(ft_fp16);
        CREATE_VAR_NUM_ITER_BY_TYPE(ft_int64);
        CREATE_VAR_NUM_ITER_BY_TYPE(ft_uint64);
        CREATE_VAR_NUM_ITER_BY_TYPE(ft_hash_64);
        CREATE_VAR_NUM_ITER_BY_TYPE(ft_hash_128);
        CREATE_VAR_NUM_ITER_BY_TYPE(ft_int8);
        CREATE_VAR_NUM_ITER_BY_TYPE(ft_uint8);
        CREATE_VAR_NUM_ITER_BY_TYPE(ft_int16);
        CREATE_VAR_NUM_ITER_BY_TYPE(ft_uint16);
        CREATE_VAR_NUM_ITER_BY_TYPE(ft_double);
    default:
        INDEXLIB_FATAL_ERROR(UnSupported, "unsupported field type [%d] "
                             "for patch iterator!", attrConfig->GetFieldType());
    }
    return NULL;
}

AttributeSegmentPatchIterator* 
AttributeSegmentPatchIteratorCreator::CreateSingleValueSegmentPatchIterator(
        const AttributeConfigPtr& attrConfig)
{
#define CREATE_SINGLE_VALUE_ITER_BY_TYPE(field_type)                    \
    case field_type:                                                    \
    {                                                                   \
        typedef FieldTypeTraits<field_type>::AttrItemType Type;         \
        return new SingleValueAttributePatchReader<Type>(attrConfig);   \
        break;                                                          \
    }                                                                   \

    switch(attrConfig->GetFieldType())
    {
        CREATE_SINGLE_VALUE_ITER_BY_TYPE(ft_int32);
        CREATE_SINGLE_VALUE_ITER_BY_TYPE(ft_uint32);
        CREATE_SINGLE_VALUE_ITER_BY_TYPE(ft_float);
        CREATE_SINGLE_VALUE_ITER_BY_TYPE(ft_fp8);
        CREATE_SINGLE_VALUE_ITER_BY_TYPE(ft_fp16);
        CREATE_SINGLE_VALUE_ITER_BY_TYPE(ft_int64);
        CREATE_SINGLE_VALUE_ITER_BY_TYPE(ft_uint64);
        CREATE_SINGLE_VALUE_ITER_BY_TYPE(ft_hash_64);
        CREATE_SINGLE_VALUE_ITER_BY_TYPE(ft_hash_128);
        CREATE_SINGLE_VALUE_ITER_BY_TYPE(ft_int8);
        CREATE_SINGLE_VALUE_ITER_BY_TYPE(ft_uint8);
        CREATE_SINGLE_VALUE_ITER_BY_TYPE(ft_int16);
        CREATE_SINGLE_VALUE_ITER_BY_TYPE(ft_uint16);
        CREATE_SINGLE_VALUE_ITER_BY_TYPE(ft_double);
    default:
        INDEXLIB_FATAL_ERROR(UnSupported, "unsupported field type [%d] "
                             "for patch iterator!", attrConfig->GetFieldType());
    }
    return NULL;
}

IE_NAMESPACE_END(index);

