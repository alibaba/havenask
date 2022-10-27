#ifndef __INDEXLIB_COMMON_DUMP_ITEM_H
#define __INDEXLIB_COMMON_DUMP_ITEM_H

#include <tr1/memory>
#include <autil/WorkItem.h>
#include <autil/mem_pool/PoolBase.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/file_system/directory.h"

IE_NAMESPACE_BEGIN(common);

class DumpItem
{
public:
    DumpItem() {}
    virtual ~DumpItem() {}

public:
    virtual void process(autil::mem_pool::PoolBase* pool) = 0;
    virtual void destroy() = 0;
    virtual void drop() = 0;
};

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_COMMON_DUMP_ITEM_H
