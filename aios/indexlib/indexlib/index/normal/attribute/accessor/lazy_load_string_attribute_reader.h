#ifndef __INDEXLIB_LAZY_LOAD_STRING_ATTRIBUTE_READER_H
#define __INDEXLIB_LAZY_LOAD_STRING_ATTRIBUTE_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/lazy_load_var_num_attribute_reader.h"
#include "indexlib/index/normal/primarykey/primary_key_index_reader.h"

IE_NAMESPACE_BEGIN(index);

class LazyLoadStringAttributeReader : public LazyLoadVarNumAttributeReader<char>
{
public:
    LazyLoadStringAttributeReader(AttributeMetrics* metrics = NULL) 
        : LazyLoadVarNumAttributeReader<char>(metrics)
    {}

    ~LazyLoadStringAttributeReader() {}

    class Creator : public AttributeReaderCreator
    {
    public:
        FieldType GetAttributeType() const 
        {
            return ft_string;
        }

        AttributeReader* Create(AttributeMetrics* metrics = NULL) const 
        {
            return new LazyLoadStringAttributeReader(metrics);
        }
    };

public:
    bool Read(docid_t docId, std::string& attrValue,
              autil::mem_pool::Pool* pool = NULL) const override;
    
    AttrType GetType() const override;
    bool IsMultiValue() const override;
};

DEFINE_SHARED_PTR(LazyLoadStringAttributeReader);

//////////////////////////////////////////////////////

inline bool LazyLoadStringAttributeReader::Read(docid_t docId, std::string& attrValue,
        autil::mem_pool::Pool* pool) const
{
    autil::MultiValueType<char> value;
    if (!LazyLoadVarNumAttributeReader<char>::Read(docId, value, pool))
    {
        return false;
    }
    attrValue = std::string(value.data(), value.size());
    return true;
}

inline AttrType LazyLoadStringAttributeReader::GetType() const
{
    return AT_STRING;
}

inline bool LazyLoadStringAttributeReader::IsMultiValue() const
{
    return false;
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_LAZY_LOAD_STRING_ATTRIBUTE_READER_H
