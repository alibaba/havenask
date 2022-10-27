#include "indexlib/partition/segment/custom_segment_dump_item.h"
#include "indexlib/common/document_deduper_helper.h"
#include "indexlib/partition/modifier/partition_modifier.h"
#include "indexlib/partition/flushed_locator_container.h"
#include "indexlib/partition/operation_queue/operation_writer.h"
#include "indexlib/partition/partition_writer_resource_calculator.h"
#include "indexlib/partition/operation_queue/operation_writer.h"
#include "indexlib/partition/custom_partition_data.h"
#include "indexlib/partition/document_deduper/document_deduper_creator.h"
#include "indexlib/index_base/index_meta/segment_file_list_wrapper.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/indexlib_file_system.h"
#include "indexlib/file_system/pack_directory.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/util/memory_control/memory_reserver.h"
#include "indexlib/table/table_common.h"
#include "indexlib/table/table_writer.h"
#include "indexlib/config/configurator_define.h"

IE_NAMESPACE_USE(misc);
using namespace std;
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(index_base);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(table);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, CustomSegmentDumpItem);

CustomSegmentDumpItem::CustomSegmentDumpItem(
        const config::IndexPartitionOptions& options,
        const config::IndexPartitionSchemaPtr& schema,
        const std::string& partitionName)
    : SegmentDumpItem(options, schema, partitionName)
    , mDumpSegmentLatencyMetric(NULL)
    , mReleaseOperationAfterDump(false)
    , mEstimateDumpSize(0u)
    , mIsDumped(false)
{
}

CustomSegmentDumpItem::~CustomSegmentDumpItem() 
{
}

void CustomSegmentDumpItem::Init(misc::MetricPtr dumpSegmentLatencyMetric,
                                 const file_system::DirectoryPtr& rootDir,
                                 const PartitionModifierPtr& modifier,
                                 const FlushedLocatorContainerPtr& flushedLocatorContainer,
                                 bool releaseOperationAfterDump,
                                 const NewSegmentMetaPtr& buildingSegMeta,
                                 const file_system::DirectoryPtr& buildingSegDir, 
                                 const file_system::PackDirectoryPtr& buildingSegPackDir,
                                 const table::TableWriterPtr& tableWriter)
{
    mDumpSegmentLatencyMetric = dumpSegmentLatencyMetric;
    mModifier = modifier;
    mFlushedLocatorContainer = flushedLocatorContainer;
    mDirectory = rootDir;
    mFileSystem = mDirectory->GetFileSystem();
    mReleaseOperationAfterDump = releaseOperationAfterDump;
    mBuildingSegMeta = buildingSegMeta;
    mBuildingSegDir = buildingSegDir;
    mBuildingSegPackDir = buildingSegPackDir;
    mTableWriter = tableWriter;
    mEstimateDumpSize = EstimateDumpMemoryUse() + EstimateDumpFileSize();
    mIsDumped.store(false);;
}

void CustomSegmentDumpItem::Dump()
{
    ScopeLatencyReporter scopeTime(mDumpSegmentLatencyMetric.get());
    mFileSystem->Sync(true);
    mFileSystem->CleanCache();

    mTableWriter->UpdateMemoryUse();
    // do actual dump
    if (!mTableWriter->DumpSegment(mSegDescription))
    {
        INDEXLIB_FATAL_ERROR(Runtime, "dump segment[%d] failed for table[%s]",
                             mBuildingSegMeta->segmentDataBase->GetSegmentId(),
                             mSchema->GetSchemaName().c_str());
    }
    mTableWriter->UpdateMemoryUse();
    SegmentInfoPtr segInfoPtr(new SegmentInfo(mBuildingSegMeta->segmentInfo));
    StoreSegmentMetas(segInfoPtr, mSegDescription);
    auto flushFuture = mDirectory->Sync(false);
    if (mFlushedLocatorContainer)
    {
        document::Locator locator = mBuildingSegMeta->segmentInfo.locator;
        mFlushedLocatorContainer->Push(flushFuture, locator);
    }
    mIsDumped.store(true);;
}

void CustomSegmentDumpItem::StoreSegmentMetas(
    const SegmentInfoPtr& segmentInfo,
    BuildSegmentDescription& segDescription)
{
    // TODO: store counterMap
    if (mBuildingSegPackDir)
    {
        mBuildingSegPackDir->ClosePackageFileWriter();
        SegmentFileListWrapper::Dump(mBuildingSegDir, segmentInfo);
    }
    else
    {
        if (segDescription.useSpecifiedDeployFileList)
        {
            fslib::FileList customFileList;
            customFileList.reserve(segDescription.deployFileList.size() + 1);
            for (const auto& file: segDescription.deployFileList)
            {
                customFileList.push_back(
                    storage::FileSystemWrapper::JoinPath(CUSTOM_DATA_DIR_NAME, file));
            }
            customFileList.push_back(
                storage::FileSystemWrapper::NormalizeDir(CUSTOM_DATA_DIR_NAME));
            SegmentFileListWrapper::Dump(
                mBuildingSegDir, customFileList, segmentInfo);
        }
        else
        {
            SegmentFileListWrapper::Dump(mBuildingSegDir, segmentInfo);
        }
    }
    segmentInfo->Store(mBuildingSegDir);
}

size_t CustomSegmentDumpItem::EstimateDumpMemoryUse() const
{
    return mTableWriter->EstimateDumpMemoryUse();
}

size_t CustomSegmentDumpItem::EstimateDumpFileSize() const
{
    return mTableWriter->EstimateDumpFileSize();
}

uint64_t CustomSegmentDumpItem::GetInMemoryMemUse() const
{
    return mTableWriter->GetCurrentMemoryUse();
}

uint64_t CustomSegmentDumpItem::GetTotalMemoryUse() const
{
    return mTableWriter->GetCurrentMemoryUse()
           + mTableWriter->EstimateDumpFileSize()
           + mTableWriter->EstimateDumpMemoryUse();
}


segmentid_t CustomSegmentDumpItem::GetSegmentId() const
{
    return mBuildingSegMeta->segmentDataBase->GetSegmentId();
}

index_base::SegmentInfoPtr CustomSegmentDumpItem::GetSegmentInfo() const
{
    return SegmentInfoPtr(new SegmentInfo(mBuildingSegMeta->segmentInfo));
}


IE_NAMESPACE_END(partition);

