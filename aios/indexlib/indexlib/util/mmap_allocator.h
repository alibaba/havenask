#ifndef __INDEXLIB_MMAP_ALLOCATOR_H
#define __INDEXLIB_MMAP_ALLOCATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include <autil/mem_pool/ChunkAllocatorBase.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/fcntl.h>
#include <sys/types.h>
#include <unistd.h>

IE_NAMESPACE_BEGIN(util);

class MMapAllocator : public autil::mem_pool::ChunkAllocatorBase
{
public:
    MMapAllocator();
    ~MMapAllocator();
public:
    virtual void* doAllocate(size_t numBytes)
    {
        return Mmap(numBytes);
    }
    virtual void doDeallocate(void* const addr, size_t numBytes)
    {
        Munmap(addr, numBytes);
    }

    static void* Mmap(size_t numBytes)
    {
        void* addr = mmap(0, numBytes, PROT_READ | PROT_WRITE,
                          MAP_ANONYMOUS | MAP_PRIVATE, 0, 0);
        if (unlikely(addr == MAP_FAILED))
        {
            IE_LOG(ERROR, "mmap numBytes[%lu] fail, %s.",
                   numBytes, strerror(errno));
            return NULL;
        }
        return addr;
    }
    static void Munmap(void* const addr, size_t numBytes)
    {
        char* const base = (char* const)addr;
        for (size_t offset = 0; offset < numBytes; offset += MUNMAP_SLICE_SIZE)
        {
            size_t actualLen = std::min(numBytes - offset, MUNMAP_SLICE_SIZE);
            if (munmap(base + offset, actualLen) < 0)
            {
                IE_LOG(ERROR, "munmap numBytes[%lu] offset[%lu] fail, %s.", 
                       numBytes, offset, strerror(errno));
            }
            usleep(0);
        }
    }
private:
    static const size_t MUNMAP_SLICE_SIZE;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MMapAllocator);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_MMAP_ALLOCATOR_H
