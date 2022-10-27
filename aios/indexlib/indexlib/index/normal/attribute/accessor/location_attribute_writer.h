#ifndef __INDEXLIB_LOCATION_ATTRIBUTE_WRITER_H
#define __INDEXLIB_LOCATION_ATTRIBUTE_WRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_writer.h"

IE_NAMESPACE_BEGIN(index);

class LocationAttributeWriter : public VarNumAttributeWriter<double>
{
public:
    LocationAttributeWriter(const config::AttributeConfigPtr& attrConfig)
        : VarNumAttributeWriter<double>(attrConfig)
    {
    }
    
    ~LocationAttributeWriter() {}
public:
    class Creator : public AttributeWriterCreator
    {
    public:
        FieldType GetAttributeType() const 
        {
            return ft_location;
        }
        
        AttributeWriter* Create(const config::AttributeConfigPtr& attrConfig) const 
        {
            return new LocationAttributeWriter(attrConfig);
        }
    };
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(LocationAttributeWriter);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_LOCATION_ATTRIBUTE_WRITER_H
