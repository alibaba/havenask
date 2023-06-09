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
#include "indexlib/partition/join_segment_writer.h"

#include "autil/StringUtil.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/normal/deletionmap/deletion_map_segment_writer.h"
#include "indexlib/index/normal/deletionmap/deletion_map_writer.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/partition/modifier/partition_modifier_creator.h"
#include "indexlib/partition/modifier/patch_modifier.h"
#include "indexlib/partition/modifier/sub_doc_modifier.h"
#include "indexlib/partition/on_disk_partition_data.h"
#include "indexlib/partition/operation_queue/operation_replayer.h"
#include "indexlib/partition/operation_queue/reopen_operation_redo_strategy.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/util/ExpandableBitmap.h"
#include "indexlib/util/memory_control/BlockMemoryQuotaController.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"
#include "indexlib/util/memory_control/MemoryReserver.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::file_system;
using namespace indexlib::common;
using namespace indexlib::index_base;
using namespace indexlib::util;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, JoinSegmentWriter);

JoinSegmentWriter::JoinSegmentWriter(const IndexPartitionSchemaPtr& schema, const IndexPartitionOptions& options,
                                     const IFileSystemPtr& fileSystem,
                                     const PartitionMemoryQuotaControllerPtr& memController)
    : mOptions(options)
    , mFileSystem(fileSystem)
    , mOpCursor(INVALID_SEGMENTID, -1)
    , mMaxTsInNewVersion(INVALID_TIMESTAMP)
{
    assert(memController);
    IndexPartitionOptions clonedOptions = options;
    clonedOptions.SetNeedRewriteFieldType(true);
    mSchema = SchemaAdapter::LoadAndRewritePartitionSchema(Directory::Get(mFileSystem), clonedOptions,
                                                           schema->GetSchemaVersionId());

    BlockMemoryQuotaControllerPtr blockMemController(new BlockMemoryQuotaController(memController, "join_seg_writer"));
    mMemController.reset(new SimpleMemoryQuotaController(blockMemController));
}

JoinSegmentWriter::~JoinSegmentWriter() {}

void JoinSegmentWriter::Init(const PartitionDataPtr& partitionData, const Version& newVersion,
                             const Version& lastVersion)
{
    // TODO: update redo use diff version
    mOnDiskVersion = newVersion;
    mLastVersion = lastVersion;
    mReadPartitionData = partitionData;
    mNewPartitionData = OnDiskPartitionData::CreateOnDiskPartitionData(mFileSystem, mSchema, mOnDiskVersion, "", true);
    mRedoStrategy.reset(new ReopenOperationRedoStrategy());
    mRedoStrategy->Init(mNewPartitionData, mLastVersion, mOptions.GetOnlineConfig(), mSchema);
    mModifier = PartitionModifierCreator::CreatePatchModifier(mSchema, mNewPartitionData, true,
                                                              mOptions.GetBuildConfig().enablePackageFile);
    if (mModifier) {
        mModifier->SetDumpThreadNum(mOptions.GetBuildConfig().dumpThreadCount);
    }
}

DeletionMapWriterPtr JoinSegmentWriter::GetDeletionMapWriter(const PartitionModifierPtr& modifier)
{
    PatchModifierPtr patchModifier;
    if (!mSchema->GetSubIndexPartitionSchema()) {
        patchModifier = DYNAMIC_POINTER_CAST(PatchModifier, modifier);
    } else {
        SubDocModifierPtr subDocModifier = DYNAMIC_POINTER_CAST(SubDocModifier, modifier);
        patchModifier = DYNAMIC_POINTER_CAST(PatchModifier, subDocModifier->GetMainModifier());
    }
    if (!patchModifier) {
        IE_LOG(ERROR, "Cannot cast modifer to PatchModifier");
        return DeletionMapWriterPtr();
    }
    return patchModifier->GetDeletionMapWriter();
}

bool JoinSegmentWriter::PreJoin()
{
    if (!mModifier) {
        return true;
    }

    // mReadPartitionData may change when rt dump segment, Clone() is thread safe
    PartitionDataPtr preJoinPartData(mReadPartitionData->Clone());
    OperationReplayer opReplayer(preJoinPartData, mSchema, mMemController);

    IE_LOG(INFO, "Begin PreJoin, redo operations!");
    if (!opReplayer.RedoOperations(mModifier, mOnDiskVersion, mRedoStrategy)) {
        IE_LOG(WARN, "PreJoin Failed!");
        return false;
    }
    mOpCursor = opReplayer.GetCursor();
    IE_LOG(INFO, "End PreJoin, redo operations!");
    return true;
}

bool JoinSegmentWriter::Join()
{
    if (!mModifier) {
        return true;
    }

    OperationReplayer opReplayer(mReadPartitionData, mSchema, mMemController);
    opReplayer.Seek(mOpCursor);
    IE_LOG(INFO, "Begin Join, redo operations!")
    if (!opReplayer.RedoOperations(mModifier, mOnDiskVersion, mRedoStrategy)) {
        IE_LOG(WARN, "Join Failed!");
        return false;
    }
    mOpCursor = opReplayer.GetCursor();
    IE_LOG(INFO, "End Join, redo operations!");
    return JoinDeletionMap();
}

bool JoinSegmentWriter::JoinDeletionMap()
{
    IE_LOG(INFO, "Begin JoinDeletionMap");
    const auto& segIdSet = mRedoStrategy->GetSkipDeleteSegments();
    if (segIdSet.empty()) {
        IE_LOG(INFO, "No DeletionMap need to be joined");
        return true;
    }

    string segListStr = autil::StringUtil::toString(segIdSet.begin(), segIdSet.end(), ",");
    IE_LOG(INFO, "DeletionMap of SegList{%s} need to be joined", segListStr.c_str());

    DeletionMapReaderPtr lastDelMapReader = mReadPartitionData->GetDeletionMapReader();
    DeletionMapWriterPtr delWriter = GetDeletionMapWriter(mModifier);

    for (const auto& segId : segIdSet) {
        DeletionMapSegmentWriterPtr delSegWriter = delWriter->GetSegmentWriter(segId);
        if (!delSegWriter) {
            IE_LOG(ERROR, "missing DeletionMapSegmentWriter[%d] in new version", segId);
            return false;
        }
        const ExpandableBitmap* oldBitmap = lastDelMapReader->GetSegmentDeletionMap(segId);
        if (!oldBitmap) {
            IE_LOG(ERROR, "missing ExpandableBitmap[%d] in last version", segId);
            return false;
        }
        if (!delSegWriter->MergeBitmap(oldBitmap, true)) {
            return false;
        }
    }
    IE_LOG(INFO, "End JoinDeletionMap");
    return true;
}

bool JoinSegmentWriter::Dump(const PartitionDataPtr& writePartitionData)
{
    if (!mModifier) {
        return true;
    }
    assert(writePartitionData);
    InMemorySegmentPtr inMemSegment = writePartitionData->CreateJoinSegment();
    assert(inMemSegment);
    const util::BuildResourceMetricsPtr& buildResMetrics = mModifier->GetBuildResourceMetrics();
    int64_t dumpFileSize = buildResMetrics->GetValue(util::BMT_DUMP_FILE_SIZE);
    MemoryReserverPtr fileMemReserver = mFileSystem->CreateMemoryReserver("join_seg_writer");
    if (unlikely(!fileMemReserver->Reserve(dumpFileSize))) {
        return false;
    }
    int64_t dumpExpandMemory = buildResMetrics->GetValue(util::BMT_DUMP_EXPAND_MEMORY_SIZE) +
                               buildResMetrics->GetValue(util::BMT_DUMP_TEMP_MEMORY_SIZE);
    int64_t freeQuota = mMemController->GetFreeQuota();
    if (freeQuota < dumpExpandMemory) {
        IE_LOG(WARN, "allocate dumpExpandMemory[%ld] failed, free quota[%ld] not enough", dumpExpandMemory, freeQuota);
        return false;
    }
    mMemController->Allocate(dumpExpandMemory);

    inMemSegment->BeginDump();
    mModifier->Dump(inMemSegment->GetDirectory(), inMemSegment->GetSegmentId());
    inMemSegment->EndDump();
    writePartitionData->CommitVersion();
    return true;
}
}} // namespace indexlib::partition
