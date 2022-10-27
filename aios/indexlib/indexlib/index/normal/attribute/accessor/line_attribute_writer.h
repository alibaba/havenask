#ifndef __INDEXLIB_LINE_ATTRIBUTE_WRITER_H
#define __INDEXLIB_LINE_ATTRIBUTE_WRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_writer.h"

IE_NAMESPACE_BEGIN(index);

class LineAttributeWriter : public VarNumAttributeWriter<double>
{
public:
    LineAttributeWriter(const config::AttributeConfigPtr& attrConfig)
        : VarNumAttributeWriter<double>(attrConfig)
    {
    }
    
    ~LineAttributeWriter() {}
public:
    class Creator : public AttributeWriterCreator
    {
    public:
        FieldType GetAttributeType() const 
        {
            return ft_line;
        }
        
        AttributeWriter* Create(const config::AttributeConfigPtr& attrConfig) const 
        {
            return new LineAttributeWriter(attrConfig);
        }
    };
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(LineAttributeWriter);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_LINE_ATTRIBUTE_WRITER_H
