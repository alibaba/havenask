#ifndef __INDEXLIB_RESOURCECONTROLWORKITEM_H
#define __INDEXLIB_RESOURCECONTROLWORKITEM_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(util);

class ResourceControlWorkItem
{
public:
    ResourceControlWorkItem() {}
    virtual ~ResourceControlWorkItem() {}

public:
    virtual int64_t GetRequiredResource() const = 0;
    virtual void Process() = 0;
    //Attention: destroy should not throw excetpion
    virtual void Destroy() = 0;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ResourceControlWorkItem);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_RESOURCECONTROLWORKITEM_H
