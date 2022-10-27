#include <autil/Lock.h>
#include <cstring>
#include "indexlib/file_system/flush_operation_queue.h"
#include "indexlib/file_system/dir_operation_cache.h"
#include "indexlib/misc/exception.h"
#include "indexlib/index_define.h"

using namespace std;
using namespace autil;
using namespace fslib;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, FlushOperationQueue);

void FlushOperationQueue::Dump()
{
    ScopedLock lock(mLock);
    IE_LOG(INFO, "Start Flush. File Operations total: %lu"
           " Dir Operations total: %lu",
           mFileFlushOperations.size(), mMkdirFlushOperations.size());

    try
    {
        RunFlushOperations(mFileFlushOperations, mMkdirFlushOperations);
        if (mFlushPromise)
        {
            mFlushPromise->set_value(true);
        }
    }
    catch(...)
    {
        if (mFlushPromise)
        {
            mFlushPromise->set_value(false);
        }
        CleanUp();
        throw;
    }
    CleanUp();
    IE_LOG(INFO, "End Flush.");
}

void FlushOperationQueue::CleanUp()
{
    FileList fileList;
    for (size_t i = 0; i < mFileFlushOperations.size(); ++i)
    {
        mFileFlushOperations[i]->GetFileNodePaths(fileList);
    }
    
    mFileFlushOperations.clear();
    mMkdirFlushOperations.clear();

    if (mMetrics)
    {
        mMetrics->DecreaseFlushMemoryUse(mFlushMemoryUse);        
    }
    mFlushMemoryUse = 0;
    
    if (mFileCache)
    {
        mFileCache->CleanFiles(fileList);
    }
    if (mFlushPromise)
    {
        mFlushPromise.reset();
    }
}

void FlushOperationQueue::PushBack(const FileFlushOperationPtr& flushOperation)
{
    ScopedLock lock(mLock);
    mFileFlushOperations.push_back(flushOperation);
    size_t flushMemUse = flushOperation->GetFlushMemoryUse();
    if (mMetrics)
    {
        mMetrics->IncreaseFlushMemoryUse(flushMemUse);
    }
    mFlushMemoryUse += flushMemUse;
}

void FlushOperationQueue::PushBack(const MkdirFlushOperationPtr& flushOperation)
{
    ScopedLock lock(mLock);
    mMkdirFlushOperations.push_back(flushOperation);
}

size_t FlushOperationQueue::Size() const
{
    return mFileFlushOperations.size() + mMkdirFlushOperations.size();
}

void FlushOperationQueue::RunFlushOperations(
        const FileFlushOperationVec& fileFlushOperation,
        const MkdirFlushOperationVec& mkDirFlushOperation)
{
    int64_t flushBeginTime = autil::TimeUtility::currentTime();
    DirOperationCache dirOperationCache(mPathMetaContainer);
    for (size_t i = 0; i < fileFlushOperation.size(); ++i)
    {
        // optimize for dfs path, add path to dir cache but not really do mkdir 
        dirOperationCache.MkParentDirIfNecessary(fileFlushOperation[i]->GetPath());
    }

    for (size_t i = 0; i < mkDirFlushOperation.size(); ++i)
    {
        // dfs scene: will mkdir only for empty path
        dirOperationCache.Mkdir(mkDirFlushOperation[i]->GetPath());
    }

    for (size_t i = 0; i < fileFlushOperation.size(); ++i)
    {
        fileFlushOperation[i]->Execute(mPathMetaContainer);
    }
    int64_t flushEndTime = autil::TimeUtility::currentTime();
    IE_LOG(INFO, "flush operations done, time[%lds]",
           (flushEndTime - flushBeginTime) / 1000 / 1000);
}

IE_NAMESPACE_END(file_system);
