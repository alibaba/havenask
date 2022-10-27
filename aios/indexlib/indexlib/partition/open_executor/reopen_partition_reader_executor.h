#ifndef __INDEXLIB_REOPEN_PARTITION_READER_EXECUTOR_H
#define __INDEXLIB_REOPEN_PARTITION_READER_EXECUTOR_H

#include <tr1/memory>
#include <autil/Lock.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/open_executor/open_executor.h"
#include "indexlib/partition/open_executor/executor_resource.h"
#include "indexlib/partition/online_partition_reader.h"
#include "indexlib/partition/patch_loader.h"

DECLARE_REFERENCE_CLASS(util, MemoryReserver);

IE_NAMESPACE_BEGIN(partition);

class ReopenPartitionReaderExecutor : public OpenExecutor
{
public:
    ReopenPartitionReaderExecutor(bool hasPreload, const std::string& partitionName = "")
        : OpenExecutor(partitionName)
        , mHasPreload(hasPreload)
    { }
    virtual ~ReopenPartitionReaderExecutor();

    bool Execute(ExecutorResource& resource) override;
    void Drop(ExecutorResource& resource) override;

    static void InitReader(ExecutorResource& resource,
                           const index_base::PartitionDataPtr& partData,
                           const PatchLoaderPtr& patchLoader,
                           const std::string& partitionName);
    static size_t EstimateReopenMemoryUse(const ExecutorResource& resource,
            const std::string& partitionName);

private:
    static bool LoadReaderPatch(const OnlinePartitionReaderPtr& reader,
                                const PatchLoaderPtr& patchLoader,
                                ExecutorResource& resource);
    static PartitionModifierPtr CreateReaderModifier(
            const IndexPartitionReaderPtr& reader);
    void InitWriter(ExecutorResource& resource, const index_base::PartitionDataPtr& partitionData);
    static void SwitchReader(ExecutorResource& resource, const OnlinePartitionReaderPtr& reader);
    bool CanLoadReader(ExecutorResource& resource,
                       const PatchLoaderPtr& patchLoader,
                       const util::MemoryReserverPtr& memReserver);
    static PatchLoaderPtr CreatePatchLoader(const ExecutorResource& resource,
                                            const index_base::PartitionDataPtr& partitionData,
                                            const std::string& partitionName);
    
private:
    bool mHasPreload;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ReopenPartitionReaderExecutor);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_REOPEN_PARTITION_READER_EXECUTOR_H
