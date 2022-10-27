#include "indexlib/index/normal/attribute/accessor/attribute_compress_info.h"
#include "indexlib/config/attribute_config.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_BEGIN(index);

AttributeCompressInfo::AttributeCompressInfo() 
{
}

AttributeCompressInfo::~AttributeCompressInfo() 
{
}

bool AttributeCompressInfo::NeedCompressData(AttributeConfigPtr attrConfig)
{
    if (!attrConfig) 
    {
        return false;
    }
    
    CompressTypeOption compressedType = attrConfig->GetCompressType();
    return compressedType.HasEquivalentCompress()
        && !attrConfig->IsMultiValue()
        && attrConfig->GetFieldType() != ft_string;
}

bool AttributeCompressInfo::NeedCompressOffset(AttributeConfigPtr attrConfig)
{
    if (!attrConfig) 
    {
        return false;
    }
    
    CompressTypeOption compressedType = attrConfig->GetCompressType();
    return compressedType.HasEquivalentCompress() && 
        (attrConfig->GetFieldType() == ft_string || attrConfig->IsMultiValue());
}

IE_NAMESPACE_END(index);

