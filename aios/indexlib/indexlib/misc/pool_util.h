#ifndef __INDEXLIB_POOL_UTIL_H
#define __INDEXLIB_POOL_UTIL_H

#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include <autil/mem_pool/PoolBase.h>

namespace heavenask { namespace indexlib {

#define IE_POOL_NEW_CLASS(pool, type, args...)          \
    (new ((pool)->allocate(sizeof(type)))type(args))

#define IE_POOL_NEW_VECTOR(pool, type, num)             \
    ((type*)((pool)->allocate(sizeof(type) * num)))

template<typename T>
static T* IE_POOL_NEW_VECTOR2(autil::mem_pool::PoolBase* pool, size_t num)
{
    if (!pool) { return new T[num]; }
    void* p = pool->allocate(sizeof(T) * num);
    return new (p) T[num];
}

template<typename T>
static void IE_POOL_DELETE_VECTOR2(autil::mem_pool::PoolBase* pool, T* value, size_t num)
{
    if (!pool) { delete [] value; return; }
    for (size_t i = 0; i < num; ++i)
    { value[i].~T(); }
    pool->deallocate((void*)value, sizeof(T) * num);
    value = NULL;
}

template<typename T>
static void IE_POOL_DELETE_CLASS(autil::mem_pool::PoolBase* pool, T *value)
{
    if (value)
    {
        value->~T();
        pool->deallocate((void *)value, sizeof(T));
    }
}

template<typename T>
static void IE_POOL_DELETE_VECTOR(autil::mem_pool::PoolBase* pool, T *value, size_t num)
{
    if (value)
    {
        pool->deallocate((void *)value, sizeof(T) * num);
    }
}

#define IE_POOL_COMPATIBLE_NEW_CLASS(pool, type, args...)               \
    ((pool) ? IE_POOL_NEW_CLASS(pool, type, args) : new type(args))

template<typename T>
static void IE_POOL_COMPATIBLE_DELETE_CLASS(autil::mem_pool::PoolBase* pool, T *&value)
{
    (pool) ? IE_POOL_DELETE_CLASS(pool, value) : (delete (value));
    value = NULL;
}

#define IE_POOL_COMPATIBLE_NEW_VECTOR(pool, type, num)                  \
    (pool) ? IE_POOL_NEW_VECTOR(pool, type, num) : new type[num]

#define IE_POOL_COMPATIBLE_DELETE_VECTOR(pool, value, num)      \
    if (!pool) { delete []value; }                              \
    else { IE_POOL_DELETE_VECTOR(pool, value, num); }           \
    value = NULL

}}

#endif //__INDEXLIB_POOL_UTIL_H
