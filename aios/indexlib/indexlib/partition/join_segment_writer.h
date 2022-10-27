#ifndef __INDEXLIB_JOIN_SEGMENT_WRITER_H
#define __INDEXLIB_JOIN_SEGMENT_WRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/partition/operation_queue/operation_cursor.h"

DECLARE_REFERENCE_CLASS(file_system, IndexlibFileSystem);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(partition, PartitionModifier);
DECLARE_REFERENCE_CLASS(partition, OperationRedoStrategy);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(util, PartitionMemoryQuotaController);
DECLARE_REFERENCE_CLASS(index, DeletionMapWriter);

IE_NAMESPACE_BEGIN(util);
template<typename T>
class SimpleMemoryQuotaControllerType;
typedef SimpleMemoryQuotaControllerType<std::atomic<int64_t>> SimpleMemoryQuotaController;
DEFINE_SHARED_PTR(SimpleMemoryQuotaController);
IE_NAMESPACE_END(util);

IE_NAMESPACE_BEGIN(partition);

class JoinSegmentWriter
{
public:
    JoinSegmentWriter(const config::IndexPartitionSchemaPtr& schema,
                      const config::IndexPartitionOptions& options,
                      const file_system::IndexlibFileSystemPtr& fileSystem,
                      const util::PartitionMemoryQuotaControllerPtr& memController);
    ~JoinSegmentWriter();

public:
    void Init(const index_base::PartitionDataPtr& partitionData, 
              const index_base::Version& newVersion,
              const index_base::Version& lastVersion);
    bool PreJoin();
    bool Join();
    bool JoinDeletionMap();
    bool Dump(const index_base::PartitionDataPtr& writePartitionData);

private:
    /* void ReportRedoCountMetric(const index::OperationQueuePtr& opQueue); */
    index::DeletionMapWriterPtr GetDeletionMapWriter(const PartitionModifierPtr& modifier);

private:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    file_system::IndexlibFileSystemPtr mFileSystem;
    util::SimpleMemoryQuotaControllerPtr mMemController;

    OperationCursor mOpCursor;
    index_base::PartitionDataPtr mReadPartitionData;
    index_base::PartitionDataPtr mNewPartitionData; 
    index_base::Version mOnDiskVersion;
    index_base::Version mLastVersion;    
    PartitionModifierPtr mModifier;
    int64_t mMaxTsInNewVersion;
    OperationRedoStrategyPtr mRedoStrategy;

private:
    friend class JoinSegmentWriterTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(JoinSegmentWriter);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_JOIN_SEGMENT_WRITER_H
