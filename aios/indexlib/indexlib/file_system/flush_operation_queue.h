#ifndef __INDEXLIB_FLUSH_OPERATION_QUEUE_H
#define __INDEXLIB_FLUSH_OPERATION_QUEUE_H

#include <tr1/memory>
#include <autil/Lock.h>
#include "indexlib/common_define.h"
#include "indexlib/file_system/dumpable.h"
#include "indexlib/file_system/file_flush_operation.h"
#include "indexlib/file_system/mkdir_flush_operation.h"
#include "indexlib/file_system/storage_metrics.h"
#include "indexlib/file_system/file_node_cache.h"
#include "indexlib/file_system/path_meta_container.h"
#include <future>

IE_NAMESPACE_BEGIN(file_system);

// for now, we only cache parent dir,
// so mkdir for index/ attribute/ .. dirs will be called ..
// need to create deletionmap dir

class FlushOperationQueue : public Dumpable
{
private:
    typedef std::vector<MkdirFlushOperationPtr> MkdirFlushOperationVec;
    typedef std::vector<FileFlushOperationPtr> FileFlushOperationVec;

public:
    FlushOperationQueue(FileNodeCache* fileNodeCache = NULL,
                        StorageMetrics* metrics = NULL,
                        const PathMetaContainerPtr& pathMetaContainer = PathMetaContainerPtr())
        : mFileCache(fileNodeCache)
        , mMetrics(metrics)
        , mPathMetaContainer(pathMetaContainer)
        , mFlushMemoryUse(0)
    {}

    ~FlushOperationQueue() {}

public:
    FlushOperationQueue(const FlushOperationQueue&) = delete;
    FlushOperationQueue& operator=(const FlushOperationQueue&) = delete;

public:
    void Dump() override;

public:
    void PushBack(const FileFlushOperationPtr& flushOperation);
    void PushBack(const MkdirFlushOperationPtr& flushOperation);
    size_t Size() const;

    void SetPromise(std::unique_ptr<std::promise<bool>>&& flushPromise)
    { mFlushPromise = std::move(flushPromise); }

private:
    void RunFlushOperations(const FileFlushOperationVec& fileFlushOperation,
                            const MkdirFlushOperationVec& mkDirFlushOperation);
    void CleanUp();
private:
    FileNodeCache* mFileCache;
    StorageMetrics* mMetrics;
    PathMetaContainerPtr mPathMetaContainer;
    int64_t mFlushMemoryUse;
    mutable autil::ThreadMutex mLock;
    FileFlushOperationVec mFileFlushOperations;
    MkdirFlushOperationVec mMkdirFlushOperations;
    std::unique_ptr<std::promise<bool>> mFlushPromise;

private:
    IE_LOG_DECLARE();
    friend class FlushOperationQueueTest;
};

DEFINE_SHARED_PTR(FlushOperationQueue);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_FLUSH_OPERATION_QUEUE_H
