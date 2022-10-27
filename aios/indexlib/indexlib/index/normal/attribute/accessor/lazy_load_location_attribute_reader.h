#ifndef __INDEXLIB_LAZY_LOAD_LOCATION_ATTRIBUTE_READER_H
#define __INDEXLIB_LAZY_LOAD_LOCATION_ATTRIBUTE_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/lazy_load_var_num_attribute_reader.h"

IE_NAMESPACE_BEGIN(index);

class LazyLoadLocationAttributeReader : public LazyLoadVarNumAttributeReader<double>
{
public:
    LazyLoadLocationAttributeReader(AttributeMetrics* metrics = NULL)
        : LazyLoadVarNumAttributeReader<double>(metrics)
    {}
    ~LazyLoadLocationAttributeReader() {}
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
            return new LazyLoadLocationAttributeReader(metrics);
        }
    };

    bool Read(docid_t docId, std::string& attrValue,
              autil::mem_pool::Pool* pool = NULL) const override
    {
        autil::CountedMultiValueType<double> value;
        bool ret = LazyLoadVarNumAttributeReader<double>::Read(docId, value, pool);
        if (ret)
        {
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
        }
        
        return ret;
    }

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(LazyLoadLocationAttributeReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_LAZY_LOAD_LOCATION_ATTRIBUTE_READER_H
