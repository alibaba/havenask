#ifndef __INDEXLIB_ATTRIBUTE_DATA_INFO_H
#define __INDEXLIB_ATTRIBUTE_DATA_INFO_H

#include <tr1/memory>
#include <autil/legacy/jsonizable.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(index);

class AttributeDataInfo : public autil::legacy::Jsonizable
{
public:
    AttributeDataInfo(uint32_t count = 0, 
                      uint32_t length = 0)
        : uniqItemCount(count)
        , maxItemLen(length)
    {}
    
    ~AttributeDataInfo() {}
    
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
    {
        json.Jsonize("uniq_item_count", uniqItemCount);
        json.Jsonize("max_item_length", maxItemLen);
    }

    std::string ToString() const
    {
        return ToJsonString(*this);
    }

    void FromString(const std::string& str)
    {
        FromJsonString(*this, str);
    }
    
public:
    uint32_t uniqItemCount;
    uint32_t maxItemLen;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributeDataInfo);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTE_DATA_INFO_H
