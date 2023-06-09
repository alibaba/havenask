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
#include "indexlib/merger/merge_partition_data.h"

#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/file_system/LocalDirectory.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/offline_segment_directory.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/index_base/segment/segment_directory_creator.h"

using namespace std;
using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::file_system;
using namespace indexlib::index_base;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, MergePartitionData);

MergePartitionData::MergePartitionData(const plugin::PluginManagerPtr& pluginManager) : mPluginManager(pluginManager) {}

MergePartitionData::~MergePartitionData() {}

MergePartitionData::MergePartitionData(const MergePartitionData& other)
    : mPartitionMeta(other.mPartitionMeta)
    , mPluginManager(other.mPluginManager)
{
    mSegmentDirectory.reset(other.mSegmentDirectory->Clone());
    if (other.mSubPartitionData) {
        mSubPartitionData.reset(other.mSubPartitionData->Clone());
    }
}

void MergePartitionData::Open(const SegmentDirectoryPtr& segDir)
{
    mSegmentDirectory = segDir;
    InitPartitionMeta(segDir->GetRootDirectory());
    SegmentDirectoryPtr subSegDir = segDir->GetSubSegmentDirectory();
    if (subSegDir) {
        mSubPartitionData.reset(new MergePartitionData(mPluginManager));
        mSubPartitionData->Open(subSegDir);
    }
}

void MergePartitionData::InitPartitionMeta(const DirectoryPtr& directory)
{
    assert(directory);
    if (directory->IsExist(INDEX_PARTITION_META_FILE_NAME)) {
        mPartitionMeta.Load(directory);
    }
}

index_base::Version MergePartitionData::GetVersion() const { return mSegmentDirectory->GetVersion(); }

index_base::Version MergePartitionData::GetOnDiskVersion() const { return mSegmentDirectory->GetOnDiskVersion(); }

segmentid_t MergePartitionData::GetLastSegmentId() const
{
    return (*(mSegmentDirectory->GetSegmentDatas().rbegin())).GetSegmentId();
}

const DirectoryPtr& MergePartitionData::GetRootDirectory() const { return mSegmentDirectory->GetRootDirectory(); }

SegmentData MergePartitionData::GetSegmentData(segmentid_t segId) const
{
    return mSegmentDirectory->GetSegmentData(segId);
}

MergePartitionData* MergePartitionData::Clone() { return new MergePartitionData(*this); }

PartitionMeta MergePartitionData::GetPartitionMeta() const
{
    if (mSegmentDirectory->IsSubSegmentDirectory()) {
        return PartitionMeta();
    }
    return mPartitionMeta;
}

const IndexFormatVersion& MergePartitionData::GetIndexFormatVersion() const
{
    assert(mSegmentDirectory);
    return mSegmentDirectory->GetIndexFormatVersion();
}

uint32_t MergePartitionData::GetIndexShardingColumnNum(const IndexPartitionOptions& options) const
{
    // TODO: online build support sharding column
    if (options.IsOnline()) {
        return 1;
    }

    uint32_t inSegColumnNum = SegmentInfo::INVALID_COLUMN_COUNT;
    for (Iterator iter = Begin(); iter != End(); iter++) {
        const std::shared_ptr<const SegmentInfo>& segInfo = iter->GetSegmentInfo();
        if (inSegColumnNum != SegmentInfo::INVALID_COLUMN_COUNT && inSegColumnNum != segInfo->shardCount) {
            INDEXLIB_FATAL_ERROR(InconsistentState, "segments with different shardCount for version[%d]!",
                                 GetOnDiskVersion().GetVersionId());
        }
        inSegColumnNum = segInfo->shardCount;
    }

    uint32_t inVersionColumnNum = GetOnDiskVersion().GetLevelInfo().GetShardCount();
    if (inSegColumnNum != SegmentInfo::INVALID_COLUMN_COUNT && inSegColumnNum != inVersionColumnNum) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "inSegColumnNum [%u] not equal with inVersionColumnNum [%u]!",
                             inSegColumnNum, inVersionColumnNum);
    }
    return inVersionColumnNum;
}

string MergePartitionData::GetLastLocator() const
{
    assert(mSegmentDirectory);
    return mSegmentDirectory->GetLastLocator();
}

PartitionSegmentIteratorPtr MergePartitionData::CreateSegmentIterator()
{
    bool isOnline = true;
    if (DYNAMIC_POINTER_CAST(OfflineSegmentDirectory, mSegmentDirectory)) {
        isOnline = false;
    }
    PartitionSegmentIteratorPtr segIter(new PartitionSegmentIterator(isOnline));
    segIter->Init(mSegmentDirectory->GetSegmentDatas(), std::vector<InMemorySegmentPtr>(), InMemorySegmentPtr());
    return segIter;
}

segmentid_t MergePartitionData::GetLastValidRtSegmentInLinkDirectory() const
{
    INDEXLIB_FATAL_ERROR(UnSupported, "not support call GetLastValidRtSegmentInLinkDirectory!");
    return INVALID_SEGMENTID;
}

bool MergePartitionData::SwitchToLinkDirectoryForRtSegments(segmentid_t lastLinkRtSegId)
{
    INDEXLIB_FATAL_ERROR(UnSupported, "not support call SwitchToLinkDirectoryForRtSegments!");
    return false;
}

DeletionMapReaderPtr MergePartitionData::GetDeletionMapReader() const
{
    INDEXLIB_FATAL_ERROR(UnSupported, "not support get deletionmap reader!");
    return DeletionMapReaderPtr();
}

PartitionInfoPtr MergePartitionData::GetPartitionInfo() const { return PartitionInfoPtr(); }

const TemperatureDocInfoPtr MergePartitionData::GetTemperatureDocInfo() const { return TemperatureDocInfoPtr(); }

}} // namespace indexlib::merger
