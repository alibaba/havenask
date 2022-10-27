#ifndef __INDEXLIB_OPERATION_REPLAYER_H
#define __INDEXLIB_OPERATION_REPLAYER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/modifier/partition_modifier.h"
#include "indexlib/partition/operation_queue/operation_cursor.h"
#include "indexlib/partition/operation_queue/operation_base.h"
#include "indexlib/partition/operation_queue/operation_iterator.h"
#include "indexlib/util/memory_control/simple_memory_quota_controller.h"
#include "indexlib/partition/operation_queue/operation_redo_strategy.h"

DECLARE_REFERENCE_CLASS(util, BlockMemoryQuotaController);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index_base, Version);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

IE_NAMESPACE_BEGIN(partition);

class OperationReplayer
{
public:
    OperationReplayer(const index_base::PartitionDataPtr& partitionData,
                      const config::IndexPartitionSchemaPtr& schema,
                      const util::SimpleMemoryQuotaControllerPtr& memController);

    ~OperationReplayer() {};
public:
    bool RedoOperations(
        const partition::PartitionModifierPtr& modifier,
        const index_base::Version& onDiskVersion,
        const OperationRedoStrategyPtr& redoStrategy);

    // redo will begin from the next position of skipCursor
    void Seek(const OperationCursor& skipCursor)
    {
        mCursor = skipCursor;
    }

    OperationCursor GetCuror()
    {
        return mCursor;
    }

public:
    // for test
    size_t GetRedoCount() const
    { return mRedoCount; }

private:
    bool RedoOneOperation(const partition::PartitionModifierPtr& modifier,
                          OperationBase* operation,
                          const OperationIterator& iter,
                          const OperationRedoHint& redoHint);

private:
    index_base::PartitionDataPtr mPartitionData;
    config::IndexPartitionSchemaPtr mSchema;
    OperationCursor mCursor;
    util::SimpleMemoryQuotaControllerPtr mMemController;
    size_t mRedoCount;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OperationReplayer);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_OPERATION_REPLAYER_H
