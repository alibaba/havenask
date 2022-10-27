#include "indexlib/partition/join_segment_writer.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/partition/operation_queue/operation_replayer.h"
#include "indexlib/partition/operation_queue/operation_redo_strategy.h"
#include "indexlib/partition/modifier/partition_modifier_creator.h"
#include "indexlib/partition/modifier/sub_doc_modifier.h"
#include "indexlib/partition/modifier/patch_modifier.h"
#include "indexlib/util/memory_control/block_memory_quota_controller.h"
#include "indexlib/util/memory_control/build_resource_metrics.h"
#include "indexlib/util/memory_control/memory_reserver.h"
#include "indexlib/util/expandable_bitmap.h"
#include "indexlib/file_system/indexlib_file_system.h"
#include "indexlib/file_system/directory_creator.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/index/normal/deletionmap/deletion_map_writer.h"
#include "indexlib/index/normal/deletionmap/deletion_map_segment_writer.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include <autil/StringUtil.h>

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, JoinSegmentWriter);

JoinSegmentWriter::JoinSegmentWriter(
        const IndexPartitionSchemaPtr& schema,
        const IndexPartitionOptions& options,
        const IndexlibFileSystemPtr& fileSystem,
        const PartitionMemoryQuotaControllerPtr& memController)
    : mOptions(options)
    , mFileSystem(fileSystem)
    , mOpCursor(INVALID_SEGMENTID, -1)
    , mMaxTsInNewVersion(INVALID_TIMESTAMP)
{
    assert(memController);
    IndexPartitionOptions clonedOptions = options;
    clonedOptions.SetNeedRewriteFieldType(true);
    mSchema = SchemaAdapter::LoadAndRewritePartitionSchema(
            DirectoryCreator::Get(mFileSystem, mFileSystem->GetRootPath(), true), 
            clonedOptions, schema->GetSchemaVersionId());

    BlockMemoryQuotaControllerPtr blockMemController(
            new BlockMemoryQuotaController(memController, "join_seg_writer"));
    mMemController.reset(new SimpleMemoryQuotaController(blockMemController));
}

JoinSegmentWriter::~JoinSegmentWriter()
{
}

void JoinSegmentWriter::Init(
        const PartitionDataPtr& partitionData, 
        const Version& newVersion,
        const Version& lastVersion)
{
    // TODO: update redo use diff version
    mOnDiskVersion = newVersion;
    mLastVersion = lastVersion;
    mReadPartitionData = partitionData;
    mNewPartitionData = 
        PartitionDataCreator::CreateOnDiskPartitionData(
                mFileSystem, mSchema, mOnDiskVersion, "", true);
    mRedoStrategy.reset(new OperationRedoStrategy());
    mRedoStrategy->Init(mNewPartitionData, mLastVersion,
                        mOptions.GetOnlineConfig(), mSchema);
    mModifier = PartitionModifierCreator::CreatePatchModifier(
        mSchema, mNewPartitionData, true,
        mOptions.GetBuildConfig().enablePackageFile);
    if (mModifier)
    {
        mModifier->SetDumpThreadNum(mOptions.GetBuildConfig().dumpThreadCount);
    }
}

DeletionMapWriterPtr JoinSegmentWriter::GetDeletionMapWriter(
    const PartitionModifierPtr& modifier)
{
    PatchModifierPtr patchModifier;    
    if (!mSchema->GetSubIndexPartitionSchema())
    {
        patchModifier = DYNAMIC_POINTER_CAST(PatchModifier, modifier);
    }
    else
    {
        SubDocModifierPtr subDocModifier = DYNAMIC_POINTER_CAST(SubDocModifier, modifier);
        patchModifier = DYNAMIC_POINTER_CAST(PatchModifier, subDocModifier->GetMainModifier());
    }
    if (!patchModifier)
    {
        IE_LOG(ERROR, "Cannot cast modifer to PatchModifier");
        return DeletionMapWriterPtr();
    }
    return patchModifier->GetDeletionMapWriter();
}

bool JoinSegmentWriter::PreJoin()
{
    if (!mModifier)
    {
        return true;
    }

    // mReadPartitionData may change when rt dump segment, Clone() is thread safe
    PartitionDataPtr preJoinPartData(mReadPartitionData->Clone());
    OperationReplayer opReplayer(preJoinPartData, mSchema, mMemController);

    IE_LOG(INFO, "Begin PreJoin, redo operations!");
    if (!opReplayer.RedoOperations(
            mModifier, mOnDiskVersion, mRedoStrategy))
    {
        IE_LOG(WARN, "PreJoin Failed!");
        return false;
    }
    mOpCursor = opReplayer.GetCuror();
    IE_LOG(INFO, "End PreJoin, redo operations!");
    return true;
}

bool JoinSegmentWriter::Join()
{
    if (!mModifier)
    {
        return true;
    }

    OperationReplayer opReplayer(mReadPartitionData, mSchema, mMemController);
    opReplayer.Seek(mOpCursor);
    IE_LOG(INFO, "Begin Join, redo operations!")
    if (!opReplayer.RedoOperations(mModifier, mOnDiskVersion, mRedoStrategy))
    {
        IE_LOG(WARN, "Join Failed!");
        return false;
    }
    mOpCursor = opReplayer.GetCuror();
    IE_LOG(INFO, "End Join, redo operations!");
    return JoinDeletionMap();
}

bool JoinSegmentWriter::JoinDeletionMap()
{
    IE_LOG(INFO, "Begin JoinDeletionMap");    
    const auto& segIdSet = mRedoStrategy->GetSkipDeleteSegments();
    if (segIdSet.empty())
    {
        IE_LOG(INFO, "No DeletionMap need to be joined");
        return true;
    }

    string segListStr = autil::StringUtil::toString(segIdSet.begin(), segIdSet.end(), ",");
    IE_LOG(INFO, "DeletionMap of SegList{%s} need to be joined", segListStr.c_str());
    
    DeletionMapReaderPtr lastDelMapReader = mReadPartitionData->GetDeletionMapReader();
    DeletionMapWriterPtr delWriter = GetDeletionMapWriter(mModifier);

    for (const auto& segId : segIdSet)
    {
        DeletionMapSegmentWriterPtr delSegWriter = delWriter->GetSegmentWriter(segId);
        if (!delSegWriter)
        {
            IE_LOG(ERROR, "missing DeletionMapSegmentWriter[%d] in new version", segId);
            return false;
        }
        const ExpandableBitmap* oldBitmap = lastDelMapReader->GetSegmentDeletionMap(segId);
        if (!oldBitmap)
        {
            IE_LOG(ERROR, "missing ExpandableBitmap[%d] in last version", segId);
            return false; 
        } 
        if (!delSegWriter->MergeBitmap(oldBitmap))
        {
            return false;             
        } 
    }
    IE_LOG(INFO, "End JoinDeletionMap");
    return true;
}

bool JoinSegmentWriter::Dump(const PartitionDataPtr& writePartitionData)
{
    if (!mModifier)
    {
        return true;
    }
    assert(writePartitionData);
    InMemorySegmentPtr inMemSegment = writePartitionData->CreateJoinSegment();
    assert(inMemSegment);
    const util::BuildResourceMetricsPtr& buildResMetrics =
        mModifier->GetBuildResourceMetrics();
    int64_t dumpFileSize = buildResMetrics->GetValue(util::BMT_DUMP_FILE_SIZE);
    MemoryReserverPtr fileMemReserver = mFileSystem->CreateMemoryReserver("join_seg_writer");
    if (unlikely(!fileMemReserver->Reserve(dumpFileSize)))
    {
        return false;
    }
    int64_t dumpExpandMemory = buildResMetrics->GetValue(util::BMT_DUMP_EXPAND_MEMORY_SIZE) +
        buildResMetrics->GetValue(util::BMT_DUMP_TEMP_MEMORY_SIZE);
    int64_t freeQuota = mMemController->GetFreeQuota();
    if (freeQuota < dumpExpandMemory)
    {
        IE_LOG(WARN, "allocate dumpExpandMemory[%ld] failed, free quota[%ld] not enough",
               dumpExpandMemory, freeQuota);
        return false;
    }
    mMemController->Allocate(dumpExpandMemory);

    inMemSegment->BeginDump();
    mModifier->Dump(inMemSegment->GetDirectory(), inMemSegment->GetSegmentId());
    inMemSegment->EndDump();
    writePartitionData->CommitVersion();
    return true;
}

IE_NAMESPACE_END(partition);

