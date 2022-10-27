#ifndef __INDEXLIB_POLYGON_ATTRIBUTE_WRITER_H
#define __INDEXLIB_POLYGON_ATTRIBUTE_WRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_writer.h"

IE_NAMESPACE_BEGIN(index);

class PolygonAttributeWriter : public VarNumAttributeWriter<double>
{
public:
    PolygonAttributeWriter(const config::AttributeConfigPtr& attrConfig)
        : VarNumAttributeWriter<double>(attrConfig)
    {
    }
    
    ~PolygonAttributeWriter() {}
public:
    class Creator : public AttributeWriterCreator
    {
    public:
        FieldType GetAttributeType() const 
        {
            return ft_polygon;
        }
        
        AttributeWriter* Create(const config::AttributeConfigPtr& attrConfig) const 
        {
            return new PolygonAttributeWriter(attrConfig);
        }
    };
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PolygonAttributeWriter);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_POLYGON_ATTRIBUTE_WRITER_H
