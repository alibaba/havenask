#ifndef __INDEXLIB_ATTRIBUTE_REFERENCE_H
#define __INDEXLIB_ATTRIBUTE_REFERENCE_H

#include <tr1/memory>
#include <autil/ConstString.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/compress_type_option.h"

IE_NAMESPACE_BEGIN(common);

class AttributeReference
{
public:
    AttributeReference(size_t offset, const std::string& attrName,
                       config::CompressTypeOption compressType,
                       int32_t fixedMultiValueCount)
        : mOffset(offset)
        , mAttrName(attrName)
        , mCompressType(compressType)
        , mFixedMultiValueCount(fixedMultiValueCount)
    {}
        
    virtual ~AttributeReference() {}
    
public:
    size_t GetOffset() const { return mOffset; }
    virtual size_t SetValue(char* baseAddr, size_t dataCursor,
                            const autil::ConstString& value) = 0;

    virtual autil::ConstString GetDataValue(const char* baseAddr) const = 0;

    const std::string& GetAttrName() const { return mAttrName; }
    
    // for test
    virtual bool GetStrValue(const char* baseAddr, std::string& value,
                             autil::mem_pool::Pool* pool) const = 0;

    virtual bool LessThan(const char* lftAddr, const char* rhtAddr) const = 0;

    int32_t GetFixMultiValueCount() const { return mFixedMultiValueCount; }

    config::CompressTypeOption GetCompressType() const { return mCompressType; }
    
protected:
    size_t mOffset;
    std::string mAttrName;
    config::CompressTypeOption mCompressType;
    int32_t mFixedMultiValueCount; // -1 means un-specified 
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributeReference);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_ATTRIBUTE_REFERENCE_H
