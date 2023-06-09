/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef __INDEXLIB_OPERATION_REPLAYER_H
#define __INDEXLIB_OPERATION_REPLAYER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/modifier/partition_modifier.h"
#include "indexlib/partition/operation_queue/operation_base.h"
#include "indexlib/partition/operation_queue/operation_cursor.h"
#include "indexlib/partition/operation_queue/operation_iterator.h"
#include "indexlib/partition/operation_queue/operation_redo_strategy.h"
#include "indexlib/util/memory_control/SimpleMemoryQuotaController.h"
namespace future_lite {
class Executor;
}

DECLARE_REFERENCE_CLASS(util, BlockMemoryQuotaController);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index_base, Version);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

namespace indexlib { namespace partition {

class OperationReplayer
{
public:
    OperationReplayer(const index_base::PartitionDataPtr& partitionData, const config::IndexPartitionSchemaPtr& schema,
                      const util::SimpleMemoryQuotaControllerPtr& memController, bool ignoreRedoFail = true);

    ~OperationReplayer() {};

public:
    bool RedoOperations(const partition::PartitionModifierPtr& modifier, const index_base::Version& onDiskVersion,
                        const OperationRedoStrategyPtr& redoStrategy);

    // redo will begin from the next position of skipCursor
    void Seek(const OperationCursor& skipCursor) { mCursor = skipCursor; }

    OperationCursor GetCursor() { return mCursor; }

public:
    // for test
    size_t GetRedoCount() const { return mRedoCount; }

private:
    bool RedoOneOperation(const partition::PartitionModifierPtr& modifier, OperationBase* operation,
                          const OperationIterator& iter, const OperationRedoHint& redoHint,
                          future_lite::Executor* executor);

private:
    index_base::PartitionDataPtr mPartitionData;
    config::IndexPartitionSchemaPtr mSchema;
    OperationCursor mCursor;
    util::SimpleMemoryQuotaControllerPtr mMemController;
    size_t mRedoCount;
    bool mIgnoreRedoFail;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OperationReplayer);
}} // namespace indexlib::partition

#endif //__INDEXLIB_OPERATION_REPLAYER_H
