#ifndef __INDEXLIB_LOCK_EXECUTOR_H
#define __INDEXLIB_LOCK_EXECUTOR_H

#include <tr1/memory>
#include <autil/Lock.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/open_executor/open_executor.h"

IE_NAMESPACE_BEGIN(partition);

class LockExecutor : public OpenExecutor
{
    typedef std::tr1::shared_ptr<autil::ScopedLock> ScopedLockPtr;
public:
    LockExecutor(ScopedLockPtr& scopedLock, autil::ThreadMutex& lock)
        : mScopedLock(scopedLock)
        , mLock(lock)
    { }

    bool Execute(ExecutorResource& resource) override
    {
        mScopedLock.reset(new autil::ScopedLock(mLock));
        return true;
    }
    void Drop(ExecutorResource& resource) override { mScopedLock.reset(); }

private:
    ScopedLockPtr& mScopedLock;
    autil::ThreadMutex& mLock;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(LockExecutor);

class UnlockExecutor : public OpenExecutor
{
    typedef std::tr1::shared_ptr<autil::ScopedLock> ScopedLockPtr;
public:
    UnlockExecutor(ScopedLockPtr& scopedLock, autil::ThreadMutex& lock)
        : mScopedLock(scopedLock)
        , mLock(lock)
    { }

    bool Execute(ExecutorResource& resource) override
    {
        mScopedLock.reset();
        return true;
    }
    void Drop(ExecutorResource& resource) override
    {
        mScopedLock.reset(new autil::ScopedLock(mLock));
    }

private:
    ScopedLockPtr& mScopedLock;
    autil::ThreadMutex& mLock;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(UnlockExecutor);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_LOCK_EXECUTOR_H
