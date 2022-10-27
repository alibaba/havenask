#include "indexlib/util/simple_pool.h"

using namespace std;

IE_NAMESPACE_BEGIN(util);

SimplePool::SimplePool()
    : _usedBytes(0)
    , _peakOfUsedBytes(0)
{ 
}

SimplePool::~SimplePool()
{ 
}

void* SimplePool::allocate(size_t numBytes)
{
    if (numBytes == 0)
    {
        return NULL;
    }
    std::set_new_handler(0);
    void* p = ::operator new(numBytes);
    if (p)
    {
        _usedBytes += numBytes;
        if (_usedBytes > _peakOfUsedBytes)
        {
            _peakOfUsedBytes = _usedBytes;
        }
    }
    return p;
}

void SimplePool::deallocate(void *ptr, size_t size)
{
    ::operator delete(ptr);
    _usedBytes -= size;
}
    
void SimplePool::release()
{
    _usedBytes = 0;
}

size_t SimplePool::reset()
{
    size_t ret = _usedBytes;
    _usedBytes = 0;
    return ret;
}

bool SimplePool::isInPool(const void *ptr) const
{
    return false;
}

IE_NAMESPACE_END(util);
