#ifndef __INDEXLIB_FLOAT_ATTRIBUTE_MERGER_CREATOR_H
#define __INDEXLIB_FLOAT_ATTRIBUTE_MERGER_CREATOR_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/index/normal/attribute/accessor/attribute_merger_creator.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_merger.h"

IE_NAMESPACE_BEGIN(index);
class FloatFp16AttributeMergerCreator : public AttributeMergerCreator
{
public:
    FieldType GetAttributeType() const 
    {
        return FieldType::ft_fp16;
    }

    AttributeMerger* Create(bool isUniqEncoded,
                            bool isUpdatable,
                            bool needMergePatch) const 
    {
        return new SingleValueAttributeMerger<int16_t>(needMergePatch);
    }
};

class FloatInt8AttributeMergerCreator : public AttributeMergerCreator
{
public:
    FieldType GetAttributeType() const 
    {
        return FieldType::ft_fp8;
    }

    AttributeMerger* Create(bool isUniqEncoded,
                            bool isUpdatable,
                            bool needMergePatch) const 
    {
        return new SingleValueAttributeMerger<int8_t>(needMergePatch);
    }
};

IE_NAMESPACE_END(index);
#endif //__INDEXLIB_FLOAT_ATTRIBUTE_MERGER_CREATOR_H
