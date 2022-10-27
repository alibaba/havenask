#ifndef __INDEXLIB_MULTI_STRING_ATTRIBUTE_WRITER_H
#define __INDEXLIB_MULTI_STRING_ATTRIBUTE_WRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_writer.h"

IE_NAMESPACE_BEGIN(index);

class MultiStringAttributeWriter : public VarNumAttributeWriter<char>
{
 public:
    MultiStringAttributeWriter(const config::AttributeConfigPtr& attrConfig)
        : VarNumAttributeWriter<char>(attrConfig)
    {
    }
    ~MultiStringAttributeWriter() {}

    DECLARE_ATTRIBUTE_WRITER_IDENTIFIER(multi_string);

public:
    class Creator : public AttributeWriterCreator
    {
    public:
        FieldType GetAttributeType() const 
        {
            return ft_string;
        }

        AttributeWriter* Create(const config::AttributeConfigPtr& attrConfig) const
        {
            return new MultiStringAttributeWriter(attrConfig);
        }        
    };
public:
    const AttributeSegmentReaderPtr CreateInMemReader() const override;
};

//////////////////////////////////////////////////////////////////

inline const AttributeSegmentReaderPtr MultiStringAttributeWriter::CreateInMemReader() const
{
    InMemVarNumAttributeReader<autil::MultiChar> * pReader = 
        new InMemVarNumAttributeReader<autil::MultiChar>(mAccessor,
                mAttrConfig->GetCompressType(),
                mAttrConfig->GetFieldConfig()->GetFixedMultiValueCount());
    return AttributeSegmentReaderPtr(pReader);
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_MULTI_STRING_ATTRIBUTE_WRITER_H
