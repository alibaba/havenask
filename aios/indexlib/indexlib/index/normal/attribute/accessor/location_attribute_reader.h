#ifndef __INDEXLIB_LOCATION_ATTRIBUTE_READER_H
#define __INDEXLIB_LOCATION_ATTRIBUTE_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_reader.h"

IE_NAMESPACE_BEGIN(index);

class LocationAttributeReader : public VarNumAttributeReader<double>
{
public:
    LocationAttributeReader(AttributeMetrics* metrics = NULL)
        : VarNumAttributeReader<double>(metrics)
    {}
    ~LocationAttributeReader() {}
public:
    class Creator : public AttributeReaderCreator
    {
    public:
        FieldType GetAttributeType() const 
        {
            return ft_location;
        }

        AttributeReader* Create(AttributeMetrics* metrics = NULL) const
        {
            return new LocationAttributeReader(metrics);
        }
    };

    bool Read(docid_t docId, std::string& attrValue,
              autil::mem_pool::Pool* pool = NULL) const override
    {
        autil::MultiValueType<double> value;
        bool ret = VarNumAttributeReader<double>::Read(docId, value, pool);
        if (!ret)
        {
            return false;
        }
        uint32_t size = value.size();
        attrValue.clear();
        for (uint32_t i= 0; i < size; i++)
        {
            std::string item = autil::StringUtil::toString<double>(value[i]);
            if (i != 0)
            {
                if (i & 1)
                {
                    attrValue += " ";
                }
                else
                {
                    attrValue += MULTI_VALUE_SEPARATOR;
                }
            }
            attrValue += item;
        }
        return true;
    }

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(LocationAttributeReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_LOCATION_ATTRIBUTE_READER_H
