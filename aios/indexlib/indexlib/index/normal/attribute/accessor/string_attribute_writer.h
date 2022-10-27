#ifndef __INDEXLIB_STRING_ATTRIBUTE_WRITER_H
#define __INDEXLIB_STRING_ATTRIBUTE_WRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_writer.h"

IE_NAMESPACE_BEGIN(index);

class StringAttributeWriter : public VarNumAttributeWriter<char>
{
public:
    StringAttributeWriter(const config::AttributeConfigPtr& attrConfig)
        : VarNumAttributeWriter<char>(attrConfig)
    {
    }
    
    ~StringAttributeWriter() {}

    DECLARE_ATTRIBUTE_WRITER_IDENTIFIER(string);

    class Creator : public AttributeWriterCreator
    {
    public:
        FieldType GetAttributeType() const 
        {
            return ft_string;
        }

        AttributeWriter* Create(const config::AttributeConfigPtr& attrConfig) const
        {
            return new StringAttributeWriter(attrConfig);
        }        
    };
};

DEFINE_SHARED_PTR(StringAttributeWriter);

///////////////////////////////////////////////////////


IE_NAMESPACE_END(index);

#endif //__INDEXLIB_STRING_ATTRIBUTE_WRITER_H
