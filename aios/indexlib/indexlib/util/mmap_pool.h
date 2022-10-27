#ifndef __INDEXLIB_MMAP_POOL_H
#define __INDEXLIB_MMAP_POOL_H

#include <tr1/memory>
#include <autil/mem_pool/PoolBase.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/mmap_allocator.h"

IE_NAMESPACE_BEGIN(util);

class MmapPool : public autil::mem_pool::PoolBase
{
public:
    MmapPool();
    ~MmapPool();

public:
    void* allocate(size_t numBytes) override;
    void deallocate(void *ptr, size_t size) override;
    void release() override;
    size_t reset() override;
    bool isInPool(const void *ptr) const override;

private:
    util::MMapAllocator mAllocator;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MmapPool);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_MMAP_POOL_H
