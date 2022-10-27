#include "indexlib/util/mmap_pool.h"

using namespace std;
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, MmapPool);

MmapPool::MmapPool() 
{
}

MmapPool::~MmapPool() 
{
}

void* MmapPool::allocate(size_t numBytes)
{
    return mAllocator.allocate(numBytes);
}

void MmapPool::deallocate(void *ptr, size_t size)
{
    mAllocator.deallocate(ptr, size);
}

void MmapPool::release()
{
    assert(false);
}

size_t MmapPool::reset()
{
    assert(false);
    return 0;
}

bool MmapPool::isInPool(const void *ptr) const
{
    assert(false);
    return false;
}


IE_NAMESPACE_END(util);

