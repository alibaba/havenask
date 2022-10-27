#ifndef __INDEXLIB_STRING_ATTRIBUTE_READER_H
#define __INDEXLIB_STRING_ATTRIBUTE_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_reader.h"
#include "indexlib/index/normal/primarykey/primary_key_index_reader.h"

IE_NAMESPACE_BEGIN(index);

class StringAttributeReader : public VarNumAttributeReader<char>
{
public:
    using VarNumAttributeReader<char>::Read;
public:
    StringAttributeReader(AttributeMetrics* metrics = NULL) 
        : VarNumAttributeReader<char>(metrics)
    {}

    ~StringAttributeReader() {}

    class Creator : public AttributeReaderCreator
    {
    public:
        FieldType GetAttributeType() const 
        {
            return ft_string;
        }

        AttributeReader* Create(AttributeMetrics* metrics = NULL) const 
        {
            return new StringAttributeReader(metrics);
        }
    };

public:
    bool Read(docid_t docId, std::string& attrValue,
              autil::mem_pool::Pool* pool = NULL) const override;
    
    AttrType GetType() const override;
    bool IsMultiValue() const override;

};

DEFINE_SHARED_PTR(StringAttributeReader);

//////////////////////////////////////////////////////

inline bool StringAttributeReader::Read(docid_t docId, std::string& attrValue,
                                        autil::mem_pool::Pool* pool) const
{
    autil::CountedMultiValueType<char> value;
    if (!VarNumAttributeReader<char>::Read(docId, value, pool))
    {
        return false;
    }
    // TODO: use MultiValueReader for performance  
    attrValue = std::string(value.data(), value.size());
    return true;
}

inline AttrType StringAttributeReader::GetType() const
{
    return AT_STRING;
}

inline bool StringAttributeReader::IsMultiValue() const
{
    return false;
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_STRING_ATTRIBUTE_READER_H
