#ifndef __INDEXLIB_OBJECT_POOL_H
#define __INDEXLIB_OBJECT_POOL_H

#include <tr1/memory>
#include <sstream>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include "indexlib/misc/exception.h"
#include "indexlib/util/profiling.h"

IE_NAMESPACE_BEGIN(util);

template <class T>
class ObjectPool
{
public:
    ObjectPool();
    ~ObjectPool();

public:
    void Init(int32_t elemCount);
    T* Alloc();
    void Free(T* object);
    uint32_t GetElemCount() const { return mElemCount; }
private:
    void DoInit();
private:
    T* mObjects;
    uint32_t* mFreeObjectIndexes;
    int32_t mFreeCursor;
    int32_t mElemCount;
    bool mInited;
private:
    friend class ObjectPoolTest;
private:
    IE_LOG_DECLARE();
};

template<class T>
inline ObjectPool<T>::ObjectPool()
    : mObjects(NULL)
    , mFreeObjectIndexes(NULL)
    , mFreeCursor(-1)
    , mElemCount(0)
    , mInited(false)
{
}

template<class T>
inline ObjectPool<T>::~ObjectPool()
{
    delete []mObjects;
    PROCESS_PROFILE_COUNT(memory, ObjectPool, -mElemCount * sizeof(T));
    delete []mFreeObjectIndexes;
    {
        PROCESS_PROFILE_COUNT(memory, ObjectPool,
                              -mElemCount * sizeof(uint16_t));
    }
}

template<class T>
inline void ObjectPool<T>::Init(int32_t elemCount)
{
    mElemCount = elemCount;
}

template<class T>
inline void ObjectPool<T>::DoInit()
{
    if (mObjects)
    {
        delete []mObjects;
    }
    if (mFreeObjectIndexes)
    {
        delete []mFreeObjectIndexes;
    }

    mFreeCursor = mElemCount - 1;
    mObjects = new T[mElemCount];
    PROCESS_PROFILE_COUNT(memory, ObjectPool, mElemCount * sizeof(T));

    mFreeObjectIndexes = new uint32_t[mElemCount];
    {
        PROCESS_PROFILE_COUNT(memory, ObjectPool, mElemCount * sizeof(uint16_t));
    }
    for (int i = 0; i < mElemCount; ++i)
    {
        mObjects[i].SetOwner(this);
        mFreeObjectIndexes[i] = i;
    }
    mInited = true;
}

template<class T>
inline T* ObjectPool<T>::Alloc()
{
    if (unlikely(!mInited))
    {
        DoInit();
    }
    assert(mFreeCursor >= 0);
    return mObjects + mFreeObjectIndexes[mFreeCursor--];
}

template<class T> 
inline void ObjectPool<T>::Free(T* object)
{
    assert(mFreeCursor < mElemCount - 1);
    assert(object - mObjects >= 0 && object - mObjects < mElemCount);
    
    mFreeObjectIndexes[++mFreeCursor] = object - mObjects;
}

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_OBJECT_POOL_H
