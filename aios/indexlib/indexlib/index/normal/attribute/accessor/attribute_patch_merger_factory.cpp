#include "indexlib/index/normal/attribute/accessor/attribute_patch_merger_factory.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_patch_merger.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_patch_merger.h"
#include "indexlib/util/class_typed_factory.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, AttributePatchMergerFactory);

AttributePatchMergerFactory::AttributePatchMergerFactory() 
{
}

AttributePatchMergerFactory::~AttributePatchMergerFactory() 
{
}

#define CREATE_ATTRIBUTE_PATCH_MERGER(fieldType, valueType)             \
    case fieldType:                                                     \
    {                                                                   \
        if (isMultiValue)                                               \
        {                                                               \
            return new VarNumAttributePatchMerger<valueType>(           \
                    attributeConfig, segUpdateBitmap);                  \
        }                                                               \
        else                                                            \
        {                                                               \
            return new SingleValueAttributePatchMerger<valueType>(      \
                    attributeConfig, segUpdateBitmap);                  \
        }                                                               \
    }


AttributePatchMerger* AttributePatchMergerFactory::Create(
    const AttributeConfigPtr& attributeConfig,
    const SegmentUpdateBitmapPtr& segUpdateBitmap)
{

    FieldType fieldType = attributeConfig->GetFieldType();
    bool isMultiValue = attributeConfig->IsMultiValue();
    switch (fieldType)
    {
        CREATE_ATTRIBUTE_PATCH_MERGER(ft_int8, int8_t);
        CREATE_ATTRIBUTE_PATCH_MERGER(ft_uint8, uint8_t);
        CREATE_ATTRIBUTE_PATCH_MERGER(ft_int16, int16_t);
        CREATE_ATTRIBUTE_PATCH_MERGER(ft_uint16, uint16_t);
        CREATE_ATTRIBUTE_PATCH_MERGER(ft_int32, int32_t);
        CREATE_ATTRIBUTE_PATCH_MERGER(ft_uint32, uint32_t);
        CREATE_ATTRIBUTE_PATCH_MERGER(ft_int64, int64_t);
        CREATE_ATTRIBUTE_PATCH_MERGER(ft_hash_64, uint64_t);
        CREATE_ATTRIBUTE_PATCH_MERGER(ft_uint64, uint64_t);
        CREATE_ATTRIBUTE_PATCH_MERGER(ft_float, float);
        CREATE_ATTRIBUTE_PATCH_MERGER(ft_fp8, int8_t);
        CREATE_ATTRIBUTE_PATCH_MERGER(ft_fp16, int16_t);
        CREATE_ATTRIBUTE_PATCH_MERGER(ft_double, double);
        CREATE_ATTRIBUTE_PATCH_MERGER(ft_hash_128, autil::uint128_t);
        case ft_string:
        {
            if (isMultiValue)
            {
                return new VarNumAttributePatchMerger<MultiChar>(attributeConfig);
            }
            return new VarNumAttributePatchMerger<char>(attributeConfig);
        }
        default:
            assert(false);
    }
    return NULL;
}

IE_NAMESPACE_END(index);

