#ifndef __INDEXLIB_FLOAT_ATTRIBUTE_WRITER_CREATOR_H
#define __INDEXLIB_FLOAT_ATTRIBUTE_WRITER_CREATOR_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <autil/mem_pool/Pool.h>
#include <tr1/memory>
#include "indexlib/index/normal/attribute/accessor/attribute_writer.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_writer.h"

IE_NAMESPACE_BEGIN(index);

class FloatFp16AttributeWriterCreator : public AttributeWriterCreator
{
public:
    FieldType GetAttributeType() const 
    {
        return FieldType::ft_fp16;
    }

    AttributeWriter* Create(const config::AttributeConfigPtr& attrConfig) const 
    {
        return new SingleValueAttributeWriter<int16_t>(attrConfig);
    }
};

class FloatInt8AttributeWriterCreator : public AttributeWriterCreator
{
public:
    FieldType GetAttributeType() const 
    {
        return FieldType::ft_fp8;
    }

    AttributeWriter* Create(const config::AttributeConfigPtr& attrConfig) const 
    {
        return new SingleValueAttributeWriter<int8_t>(attrConfig);
    }
};

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_FLOAT_ATTRIBUTE_WRITER_CREATOR_H
