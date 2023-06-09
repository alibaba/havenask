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
#include "indexlib/partition/open_executor/realtime_index_recover_executor.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/index_base/schema_rewriter.h"
#include "indexlib/partition/modifier/partition_modifier_creator.h"
#include "indexlib/partition/online_partition_reader.h"
#include "indexlib/partition/open_executor/release_partition_reader_executor.h"
#include "indexlib/partition/operation_queue/operation_replayer.h"
#include "indexlib/partition/operation_queue/recover_rt_operation_redo_strategy.h"
#include "indexlib/util/memory_control/SimpleMemoryQuotaController.h"

using namespace std;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, RealtimeIndexRecoverExecutor);

RealtimeIndexRecoverExecutor::RealtimeIndexRecoverExecutor() {}

RealtimeIndexRecoverExecutor::~RealtimeIndexRecoverExecutor() {}

bool RealtimeIndexRecoverExecutor::Execute(ExecutorResource& resource)
{
    IE_LOG(INFO, "begin recover rt index");
    auto tableType = resource.mSchema->GetTableType();
    if (tableType == tt_kkv || tableType == tt_kv) {
        // only index table type support redo
        return true;
    }
    // todo clone schema with all info
    config::IndexPartitionSchemaPtr cloneSchema(resource.mSchema->Clone());
    auto rootDir = file_system::Directory::Get(resource.mFileSystem);
    index_base::SchemaAdapter::RewritePartitionSchema(cloneSchema, rootDir, resource.mOptions);

    index_base::SchemaRewriter::DisablePKLoadParam(cloneSchema);
    resource.mSchema = cloneSchema;
    auto realtimeReader = ReleasePartitionReaderExecutor::CreateRealtimeReader(resource);
    PartitionModifierPtr modifier = PartitionModifierCreator::CreateInplaceModifier(resource.mSchema, realtimeReader);
    util::BlockMemoryQuotaControllerPtr blockMemController(
        new util::BlockMemoryQuotaController(resource.mPartitionMemController, "realtime_index_recover"));
    util::SimpleMemoryQuotaControllerPtr memController(new util::SimpleMemoryQuotaController(blockMemController));
    OperationReplayer replayer(resource.mPartitionDataHolder.Get(), resource.mSchema, memController, false);
    OperationRedoStrategyPtr redoStrategy(new RecoverRtOperationRedoStrategy(realtimeReader->GetVersion()));
    bool ret = replayer.RedoOperations(modifier, index_base::Version(INVALID_VERSION), redoStrategy);
    IE_LOG(INFO, "end recover rt index");
    return ret;
}
}} // namespace indexlib::partition
