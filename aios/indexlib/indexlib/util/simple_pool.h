#ifndef __INDEXLIB_SIMPLE_POOL_H
#define __INDEXLIB_SIMPLE_POOL_H

#include <tr1/memory>
#include <autil/mem_pool/PoolBase.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(util);

class SimplePool : public autil::mem_pool::PoolBase
{
public:
    SimplePool();
    virtual ~SimplePool();
private:
    SimplePool(const SimplePool &);
    SimplePool& operator = (const SimplePool &);
public:
    void* allocate(size_t numBytes) override;
    void deallocate(void *ptr, size_t size) override;
    void release() override;
    size_t reset() override;
    bool isInPool(const void *ptr) const override;

    size_t getUsedBytes() const
    { return _usedBytes; }
    size_t getPeakOfUsedBytes() const
    { return _peakOfUsedBytes; }

private:
    size_t _usedBytes;
    size_t _peakOfUsedBytes;
};

DEFINE_SHARED_PTR(SimplePool);
IE_NAMESPACE_END(util);

#endif //__INDEXLIB_SIMPLE_POOL_H
