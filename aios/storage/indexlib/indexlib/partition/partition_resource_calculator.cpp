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
#include "indexlib/partition/partition_resource_calculator.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/index_locator.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/index/calculator/on_disk_segment_size_calculator.h"
#include "indexlib/index/calculator/partition_size_calculator.h"
#include "indexlib/index/normal/attribute/accessor/pack_attr_update_doc_size_calculator.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/online_join_policy.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/partition/on_disk_partition_data.h"
#include "indexlib/partition/online_partition_metrics.h"
#include "indexlib/partition/operation_queue/operation_iterator.h"
#include "indexlib/partition/partition_data_creator.h"
#include "indexlib/partition/patch_loader.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/util/counter/MultiCounter.h"

using namespace std;
using namespace autil;
using namespace indexlib::document;
using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::file_system;
using namespace indexlib::index_base;
using namespace indexlib::plugin;
using namespace indexlib::util;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, PartitionResourceCalculator);

void PartitionResourceCalculator::Init(const DirectoryPtr& rootDirectory, const PartitionWriterPtr& writer,
                                       const InMemorySegmentContainerPtr& container,
                                       const PluginManagerPtr& pluginManager)
{
    autil::ScopedLock lock(mLock);
    mRootDirectory = rootDirectory;
    mWriter = writer;
    mInMemSegContainer = container;
    mPluginManager = pluginManager;
}

void PartitionResourceCalculator::CalculateTemperatureIndexSize(const index_base::PartitionDataPtr& partitionData,
                                                                const IndexPartitionSchemaPtr& schema,
                                                                const Version& version, int64_t& hotSize,
                                                                int64_t& warmSize, int64_t& coldSize)
{
    if (!partitionData) {
        return;
    }
    OnDiskSegmentSizeCalculator calculator;
    size_t segmentIndexSize = 0;
    index_base::SegmentDirectoryPtr segmentDirector(partitionData->GetSegmentDirectory()->Clone());
    for (auto& segmentData : segmentDirector->GetSegmentDatas()) {
        const auto segId = segmentData.GetSegmentId();
        SegmentTemperatureMeta meta;
        if (version.GetSegmentTemperatureMeta(segId, meta)) {
            segmentIndexSize = calculator.GetSegmentSize(segmentData, schema);
            if (meta.segTemperature == "HOT") {
                hotSize += segmentIndexSize;
            } else if (meta.segTemperature == "WARM") {
                warmSize += segmentIndexSize;
            } else {
                coldSize += segmentIndexSize;
            }
        }
    }
}

void PartitionResourceCalculator::CalculateCurrentIndexSize(const PartitionDataPtr& partitionData,
                                                            const IndexPartitionSchemaPtr& schema) const
{
    if (!partitionData) {
        return;
    }
    std::map<segmentid_t, uint64_t> tmpSegId2IndexSize;
    std::map<segmentid_t, uint64_t> newSegId2IndexSize;
    {
        autil::ScopedLock lock(mLock);
        tmpSegId2IndexSize = mSegId2IndexSize;
    }
    size_t segmentIndexSize = 0;
    size_t totalSegmentIndexSize = 0;
    OnDiskSegmentSizeCalculator calculator;
    index_base::SegmentDirectoryPtr segmentDirector(partitionData->GetSegmentDirectory()->Clone());
    for (auto& segmentData : segmentDirector->GetSegmentDatas()) {
        const auto segId = segmentData.GetSegmentId();
        auto it = tmpSegId2IndexSize.find(segId);
        if (it != tmpSegId2IndexSize.end()) {
            segmentIndexSize = it->second;
        } else {
            segmentIndexSize = calculator.GetSegmentSize(segmentData, schema);
        }
        totalSegmentIndexSize += segmentIndexSize;
        newSegId2IndexSize[segId] = segmentIndexSize;
    }

    autil::ScopedLock lock(mLock);
    mSegId2IndexSize.swap(newSegId2IndexSize);
    mCurrentIndexSize = totalSegmentIndexSize;
}

size_t PartitionResourceCalculator::GetCurrentMemoryUse() const
{
    return GetRtIndexMemoryUse() + GetIncIndexMemoryUse() + GetWriterMemoryUse() + GetOldInMemorySegmentMemoryUse();
}

size_t PartitionResourceCalculator::GetRtIndexMemoryUse() const
{
    assert(mRootDirectory);
    const IFileSystemPtr& fileSystem = mRootDirectory->GetFileSystem();

    assert(fileSystem);
    FileSystemMetrics fsMetrics = fileSystem->GetFileSystemMetrics();
    const StorageMetrics& inputStorageMetrics = fsMetrics.GetInputStorageMetrics();
    const StorageMetrics& outputStorageMetrics = fsMetrics.GetOutputStorageMetrics();

    return inputStorageMetrics.GetInMemFileLength(FSMetricGroup::FSMG_MEM) +
           inputStorageMetrics.GetMmapLockFileLength(FSMetricGroup::FSMG_MEM) +
           inputStorageMetrics.GetResourceFileLength(FSMetricGroup::FSMG_MEM) +
           inputStorageMetrics.GetFlushMemoryUse() + outputStorageMetrics.GetInMemFileLength(FSMetricGroup::FSMG_MEM) +
           outputStorageMetrics.GetMmapLockFileLength(FSMetricGroup::FSMG_MEM) +
           outputStorageMetrics.GetResourceFileLength(FSMetricGroup::FSMG_MEM) +
           outputStorageMetrics.GetFlushMemoryUse() + mSwitchRtSegmentLockSize;
}

size_t PartitionResourceCalculator::GetBuildingSegmentDumpExpandSize() const
{
    ScopedLock lock(mLock);
    return mWriter ? mWriter->GetBuildingSegmentDumpExpandSize() : 0;
}

size_t PartitionResourceCalculator::GetWriterMemoryUse() const
{
    ScopedLock lock(mLock);
    return mWriter ? mWriter->GetTotalMemoryUse() : 0;
}

size_t PartitionResourceCalculator::GetIncIndexMemoryUse() const
{
    assert(mRootDirectory);
    const IFileSystemPtr& fileSystem = mRootDirectory->GetFileSystem();

    assert(fileSystem);
    FileSystemMetrics fsMetrics = fileSystem->GetFileSystemMetrics();
    // TODO(qingran): check ut OnlinePartitionInteTest/OnlinePartitionInteTestMode.TestCombinedPkSegmentCleanSliceFile/0
    // because now, slice file will always be in output storage
    const StorageMetrics& inputStorageMetrics = fsMetrics.GetInputStorageMetrics();
    const StorageMetrics& outputStorageMetrics = fsMetrics.GetOutputStorageMetrics();

    // TODO: actual used cache size
    size_t totalIncSize = inputStorageMetrics.GetFileLength(FSMetricGroup::FSMG_LOCAL, FSFT_MMAP_LOCK) +
                          inputStorageMetrics.GetFileLength(FSMetricGroup::FSMG_LOCAL, FSFT_SLICE) +
                          inputStorageMetrics.GetFileLength(FSMetricGroup::FSMG_LOCAL, FSFT_MEM) +
                          inputStorageMetrics.GetFileLength(FSMetricGroup::FSMG_LOCAL, FSFT_RESOURCE) +
                          outputStorageMetrics.GetFileLength(FSMetricGroup::FSMG_LOCAL, FSFT_MMAP_LOCK) +
                          outputStorageMetrics.GetFileLength(FSMetricGroup::FSMG_LOCAL, FSFT_SLICE) +
                          outputStorageMetrics.GetFileLength(FSMetricGroup::FSMG_LOCAL, FSFT_MEM) +
                          outputStorageMetrics.GetFileLength(FSMetricGroup::FSMG_LOCAL, FSFT_RESOURCE) +
                          mOnlineConfig.loadConfigList.GetTotalCacheMemorySize();
    if (totalIncSize >= mSwitchRtSegmentLockSize) {
        return totalIncSize - mSwitchRtSegmentLockSize;
    }
    IE_LOG(ERROR,
           "mSwitchRtSegmentLockSize [%lu] is bigger than totalIncSize [%lu], "
           "rootPath[%s]",
           mSwitchRtSegmentLockSize, totalIncSize, mRootDirectory->DebugString().c_str());
    return totalIncSize;
}
size_t PartitionResourceCalculator::GetOldInMemorySegmentMemoryUse() const
{
    if (!mInMemSegContainer) {
        return 0u;
    }
    return mInMemSegContainer->GetTotalMemoryUse();
}

size_t PartitionResourceCalculator::EstimateDiffVersionLockSizeWithoutPatch(
    const config::IndexPartitionSchemaPtr& schema, const file_system::DirectoryPtr& directory,
    const index_base::Version& version, const index_base::Version& lastLoadedVersion,
    const util::MultiCounterPtr& counter, bool throwExceptionIfNotExist) const
{
    PartitionSizeCalculator calculator(directory, schema, throwExceptionIfNotExist, mPluginManager);
    PartitionDataPtr partitionData;
    if (schema->GetTableType() == tt_index) {
        partitionData = OnDiskPartitionData::CreateOnDiskPartitionData(
            directory->GetFileSystem(), version, directory->GetLogicalPath(),
            schema->GetSubIndexPartitionSchema() != NULL, false);
    }

    size_t size =
        calculator.CalculateDiffVersionLockSizeWithoutPatch(version, lastLoadedVersion, partitionData, counter);
    assert(!counter || size == counter->Sum());
    return size;
}

size_t PartitionResourceCalculator::EstimateRedoOperationExpandSize(const IndexPartitionSchemaPtr& schema,
                                                                    PartitionDataPtr& partitionData,
                                                                    int64_t timestamp) const
{
    size_t redoOperationExpandSizeInBytes = 0;
    PartitionData::Iterator iter = partitionData->Begin();
    for (; iter != partitionData->End(); iter++) {
        SegmentData segData = *iter;
        DirectoryPtr opDir = segData.GetOperationDirectory(false);
        if (!opDir) {
            continue;
        }

        if (opDir->IsExist(OPERATION_DATA_FILE_NAME)) {
            redoOperationExpandSizeInBytes += opDir->GetFileLength(OPERATION_DATA_FILE_NAME);
        }
    }

    if (mOnlineConfig.onDiskFlushRealtimeIndex) {
        // TODO: make sure only one segment operation load to in memory file
        redoOperationExpandSizeInBytes +=
            OperationIterator::GetMaxUnObsoleteSegmentOperationSize(partitionData, timestamp);
    }
    PackAttrUpdateDocSizeCalculator packUpdateCalculator(partitionData, schema);
    redoOperationExpandSizeInBytes += packUpdateCalculator.EstimateUpdateDocSizeInUnobseleteRtSeg(timestamp);
    return redoOperationExpandSizeInBytes;
}

void PartitionResourceCalculator::UpdateSwitchRtSegments(const config::IndexPartitionSchemaPtr& schema,
                                                         const vector<segmentid_t>& segIds)
{
    assert(mRootDirectory);
    if (mRootDirectory->GetRootLinkPath().empty()) {
        IE_LOG(INFO, "Not use root link");
        return;
    }

    ScopedLock lock(mLock);
    if (mSwitchRtSegIds == segIds) {
        IE_LOG(DEBUG, "mSwitchRtSegIds == segIds, mSwitchRtSegIds size: %lu", mSwitchRtSegIds.size());
        return;
    }

    string linkName = mRootDirectory->GetRootLinkPath();
    DirectoryPtr linkDir = mRootDirectory->GetDirectory(linkName, false);
    if (!linkDir) {
        IE_LOG(ERROR, "%s not exist in [%s]!", linkName.c_str(), mRootDirectory->DebugString().c_str());
        return;
    }

    DirectoryPtr rtPartitionDir = linkDir->GetDirectory(RT_INDEX_PARTITION_DIR_NAME, false);
    if (!rtPartitionDir) {
        IE_LOG(ERROR, "%s not exist in [%s]!", RT_INDEX_PARTITION_DIR_NAME.c_str(), linkDir->DebugString().c_str());
        return;
    }

    string segStr;
    Version version;
    for (size_t i = 0; i < segIds.size(); i++) {
        version.AddSegment(segIds[i]);
        segStr += StringUtil::toString(segIds[i]) + ";";
    }

    mSwitchRtSegmentLockSize = EstimateDiffVersionLockSizeWithoutPatch(
        schema, rtPartitionDir, version, index_base::Version(INVALID_VERSION), MultiCounterPtr(), false);
    IE_LOG(INFO, "switched rt segment lock size [%lu] for segments[%s]", mSwitchRtSegmentLockSize, segStr.c_str());
    mSwitchRtSegIds.assign(segIds.begin(), segIds.end());
}

size_t PartitionResourceCalculator::EstimateLoadPatchMemoryUse(const IndexPartitionSchemaPtr& schema,
                                                               const DirectoryPtr& directory, const Version& version,
                                                               const Version& lastLoadedVersion) const
{
    PartitionDataPtr partitionData =
        OnDiskPartitionData::CreateOnDiskPartitionData(directory->GetFileSystem(), version, directory->GetLogicalPath(),
                                                       schema->GetSubIndexPartitionSchema() != NULL, false);

    PatchLoaderPtr patchLoader(new PatchLoader(schema, mOnlineConfig));
    segmentid_t startLoadSegment =
        (lastLoadedVersion == index_base::Version(INVALID_VERSION)) ? 0 : lastLoadedVersion.GetLastSegment() + 1;

    string locatorStr = partitionData->GetLastLocator();
    bool isIncConsistentWithRt =
        (lastLoadedVersion != index_base::Version(INVALID_VERSION)) && mOnlineConfig.isIncConsistentWithRealtime;
    IndexLocator indexLocator;
    if (!indexLocator.fromString(locatorStr)) {
        isIncConsistentWithRt = false;
    } else {
        bool fromInc = false;
        OnlineJoinPolicy joinPolicy(version, schema->GetTableType(), partitionData->GetSrcSignature());
        joinPolicy.GetRtSeekTimestamp(indexLocator, fromInc);
        if (fromInc) {
            isIncConsistentWithRt = false;
        }
    }
    IE_LOG(INFO, "init patchLoader with isIncConsistentWithRt[%d]", isIncConsistentWithRt);
    patchLoader->Init(partitionData, isIncConsistentWithRt, lastLoadedVersion, startLoadSegment, false);
    return patchLoader->CalculatePatchLoadExpandSize();
}
}} // namespace indexlib::partition
