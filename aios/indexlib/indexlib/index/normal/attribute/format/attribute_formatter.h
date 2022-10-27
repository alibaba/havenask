#ifndef __INDEXLIB_ATTRIBUTE_FORMATTER_H
#define __INDEXLIB_ATTRIBUTE_FORMATTER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/slice_array/byte_aligned_slice_array.h"
#include "indexlib/common/field_format/attribute/attribute_convertor.h"
#include "indexlib/config/compress_type_option.h"

IE_NAMESPACE_BEGIN(index);

class AttributeFormatter
{
public:
    AttributeFormatter();
    virtual ~AttributeFormatter();
public:
    virtual void Init(uint32_t offset, uint32_t recordSize,
                      config::CompressTypeOption compressType) = 0;

    virtual void Set(docid_t docId, const autil::ConstString& attributeValue,
                     const util::ByteAlignedSliceArrayPtr &fixedData) = 0;

    virtual void Set(const autil::ConstString& attributeValue, uint8_t *oneDocBaseAddr)
    { assert(false); }

    virtual bool Reset(docid_t docId, const autil::ConstString& attributeValue,
                       const util::ByteAlignedSliceArrayPtr &fixedData) = 0;
    
    void SetAttrConvertor(const common::AttributeConvertorPtr& attrConvertor);
    const common::AttributeConvertorPtr& GetAttrConvertor() const { return mAttrConvertor; }

public:
    //for test
    virtual void Get(docid_t docId, const util::ByteAlignedSliceArrayPtr &fixedData, 
                     std::string& attributeValue) const
    { assert(false); }

    virtual void Get(docid_t docId, const uint8_t* &buffer, std::string& attributeValue) const
    { assert(false); }
protected:
    common::AttributeConvertorPtr mAttrConvertor;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributeFormatter);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTE_FORMATTER_H
