#ifndef __INDEXLIB_CACHE_ALLOCATOR_H
#define __INDEXLIB_CACHE_ALLOCATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(util);

class CacheAllocator
{
public:
    CacheAllocator() = default;
    ~CacheAllocator() = default;

public:
    virtual void* Allocate() = 0;
    virtual void Deallocate(void* const addr) = 0;

};

DEFINE_SHARED_PTR(CacheAllocator);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_CACHE_ALLOCATOR_H
