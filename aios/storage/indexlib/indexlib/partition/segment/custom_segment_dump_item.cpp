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
#include "indexlib/partition/segment/custom_segment_dump_item.h"

#include "indexlib/config/configurator_define.h"
#include "indexlib/config/document_deduper_helper.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/index_meta/segment_file_list_wrapper.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_base/segment/segment_writer.h"
#include "indexlib/partition/custom_partition_data.h"
#include "indexlib/partition/document_deduper/document_deduper_creator.h"
#include "indexlib/partition/flushed_locator_container.h"
#include "indexlib/partition/modifier/partition_modifier.h"
#include "indexlib/partition/operation_queue/operation_writer.h"
#include "indexlib/partition/partition_writer_resource_calculator.h"
#include "indexlib/table/table_common.h"
#include "indexlib/table/table_writer.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/memory_control/MemoryReserver.h"

using namespace std;
using namespace indexlib::file_system;
using namespace indexlib::index_base;
using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::table;
using namespace indexlib::util;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, CustomSegmentDumpItem);

CustomSegmentDumpItem::CustomSegmentDumpItem(const config::IndexPartitionOptions& options,
                                             const config::IndexPartitionSchemaPtr& schema,
                                             const std::string& partitionName)
    : SegmentDumpItem(options, schema, partitionName)
    , mDumpSegmentLatencyMetric(NULL)
    , mReleaseOperationAfterDump(false)
    , mEstimateDumpSize(0u)
    , mIsDumped(false)
{
}

CustomSegmentDumpItem::~CustomSegmentDumpItem() {}

void CustomSegmentDumpItem::Init(util::MetricPtr dumpSegmentLatencyMetric, const file_system::DirectoryPtr& rootDir,
                                 const PartitionModifierPtr& modifier,
                                 const FlushedLocatorContainerPtr& flushedLocatorContainer,
                                 bool releaseOperationAfterDump, const NewSegmentMetaPtr& buildingSegMeta,
                                 const file_system::DirectoryPtr& buildingSegDir,
                                 // TODO: @qingran package
                                 // const file_system::PackDirectoryPtr& buildingSegPackDir,
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
    mTableWriter = tableWriter;
    mEstimateDumpSize = EstimateDumpMemoryUse() + EstimateDumpFileSize();
    mIsDumped.store(false);
}

void CustomSegmentDumpItem::Dump()
{
    ScopeLatencyReporter scopeTime(mDumpSegmentLatencyMetric.get());
    mFileSystem->Sync(true).GetOrThrow();
    mFileSystem->CleanCache();

    mTableWriter->UpdateMemoryUse();
    // do actual dump
    if (!mTableWriter->DumpSegment(mSegDescription)) {
        INDEXLIB_FATAL_ERROR(Runtime, "dump segment[%d] failed for table[%s]",
                             mBuildingSegMeta->segmentDataBase->GetSegmentId(), mSchema->GetSchemaName().c_str());
    }
    mTableWriter->UpdateMemoryUse();
    SegmentInfoPtr segInfoPtr(new SegmentInfo(mBuildingSegMeta->segmentInfo));
    StoreSegmentMetas(segInfoPtr, mSegDescription);
    mDirectory->FlushPackage();
    auto flushFuture = mDirectory->Sync(false);
    if (mFlushedLocatorContainer) {
        document::Locator locator(mBuildingSegMeta->segmentInfo.GetLocator().Serialize());
        mFlushedLocatorContainer->Push(flushFuture, locator);
    }
    mIsDumped.store(true);
    ;
}

void CustomSegmentDumpItem::StoreSegmentMetas(const SegmentInfoPtr& segmentInfo,
                                              BuildSegmentDescription& segDescription)
{
    // TODO: @qingran support
    // TODO: store counterMap
    if (segDescription.useSpecifiedDeployFileList) {
        fslib::FileList customFileList;
        customFileList.reserve(segDescription.deployFileList.size() + 1);
        for (const auto& file : segDescription.deployFileList) {
            customFileList.push_back(util::PathUtil::JoinPath(CUSTOM_DATA_DIR_NAME, file));
        }
        customFileList.push_back(util::PathUtil::NormalizeDir(CUSTOM_DATA_DIR_NAME));
        SegmentFileListWrapper::Dump(mBuildingSegDir, customFileList, segmentInfo);
    } else {
        SegmentFileListWrapper::Dump(mBuildingSegDir, segmentInfo);
    }
    segmentInfo->Store(mBuildingSegDir);
}

size_t CustomSegmentDumpItem::EstimateDumpMemoryUse() const { return mTableWriter->EstimateDumpMemoryUse(); }

size_t CustomSegmentDumpItem::EstimateDumpFileSize() const { return mTableWriter->EstimateDumpFileSize(); }

uint64_t CustomSegmentDumpItem::GetInMemoryMemUse() const { return mTableWriter->GetCurrentMemoryUse(); }

uint64_t CustomSegmentDumpItem::GetTotalMemoryUse() const
{
    return mTableWriter->GetCurrentMemoryUse() + mTableWriter->EstimateDumpFileSize() +
           mTableWriter->EstimateDumpMemoryUse();
}

segmentid_t CustomSegmentDumpItem::GetSegmentId() const { return mBuildingSegMeta->segmentDataBase->GetSegmentId(); }

index_base::SegmentInfoPtr CustomSegmentDumpItem::GetSegmentInfo() const
{
    return SegmentInfoPtr(new SegmentInfo(mBuildingSegMeta->segmentInfo));
}
}} // namespace indexlib::partition
