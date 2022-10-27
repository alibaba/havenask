#ifndef __INDEXLIB_RELEASE_PARTITION_READER_EXECUTOR_H
#define __INDEXLIB_RELEASE_PARTITION_READER_EXECUTOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/open_executor/open_executor.h"

DECLARE_REFERENCE_CLASS(partition, IndexPartitionReader);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);

IE_NAMESPACE_BEGIN(partition);

class ReleasePartitionReaderExecutor : public OpenExecutor
{
public:
    ReleasePartitionReaderExecutor() { }
    ~ReleasePartitionReaderExecutor() { }

    bool Execute(ExecutorResource& resource) override;
    void Drop(ExecutorResource& resource) override
    {
        resource.mLoadedIncVersion = mLastLoadedIncVersion;
    };

private:
    IndexPartitionReaderPtr CreateRealtimeReader(ExecutorResource& resource);
    void CleanResource(ExecutorResource& resource);

private:
    index_base::Version mLastLoadedIncVersion;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ReleasePartitionReaderExecutor);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_RELEASE_PARTITION_READER_EXECUTOR_H
