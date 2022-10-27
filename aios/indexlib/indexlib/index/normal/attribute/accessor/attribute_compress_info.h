#ifndef __INDEXLIB_ATTRIBUTE_COMPRESS_INFO_H
#define __INDEXLIB_ATTRIBUTE_COMPRESS_INFO_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(config, AttributeConfig);

IE_NAMESPACE_BEGIN(index);

class AttributeCompressInfo
{
public:
    AttributeCompressInfo();
    ~AttributeCompressInfo();

public:
    static bool NeedCompressData(config::AttributeConfigPtr attrConfig);
    static bool NeedCompressOffset(config::AttributeConfigPtr attrConfig);
};

DEFINE_SHARED_PTR(AttributeCompressInfo);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTE_COMPRESS_INFO_H
