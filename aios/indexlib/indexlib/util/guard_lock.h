#ifndef __INDEXLIB_GUARD_LOCK_H
#define __INDEXLIB_GUARD_LOCK_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include <tr1/memory>

IE_NAMESPACE_BEGIN(util);

template <typename T>
class GuardLock
{
public:
    typedef T LockType;

public:
    explicit GuardLock(LockType& lock)
        : mLock(lock)
    {
        mLock.lock();
    }

    ~GuardLock()
    {
        mLock.unlock();   
    }

private:
    GuardLock(const GuardLock&);
    GuardLock& operator=(const GuardLock&);

private:
    LockType& mLock;
};

class NullLock
{
public:
    int lock() {return 0;}
    int unlock() {return 0;}
};


IE_NAMESPACE_END(util);

#endif //__INDEXLIB_GUARD_LOCK_H
