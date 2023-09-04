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
#include "indexlib/partition/on_disk_partition_data.h"

#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/LocalDirectory.h"
#include "indexlib/file_system/MountOption.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/patch/patch_file_finder_creator.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/offline_segment_directory.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/index_base/segment/segment_directory_creator.h"
#include "indexlib/partition/partition_info_holder.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"

using namespace std;
using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::file_system;
using namespace indexlib::index_base;

namespace indexlib { namespace partition {

IE_LOG_SETUP(partition, OnDiskPartitionData);

OnDiskPartitionData::OnDiskPartitionData(const plugin::PluginManagerPtr& pluginManager) : mPluginManager(pluginManager)
{
}

OnDiskPartitionData::~OnDiskPartitionData() {}

OnDiskPartitionData::OnDiskPartitionData(const OnDiskPartitionData& other)
    : mPartitionMeta(other.mPartitionMeta)
    , mDeletionMapReader(other.mDeletionMapReader)
    , mPartitionInfoHolder(other.mPartitionInfoHolder)
    , mPluginManager(other.mPluginManager)
    , mDeletionMapOption(other.mDeletionMapOption)
{
    mSegmentDirectory.reset(other.mSegmentDirectory->Clone());
    if (other.mSubPartitionData) {
        mSubPartitionData.reset(other.mSubPartitionData->Clone());
    }
}

index::DeletionMapReaderPtr OnDiskPartitionData::GetDeletionMapReader() const { return mDeletionMapReader; }

void OnDiskPartitionData::Open(const SegmentDirectoryPtr& segDir,
                               OnDiskPartitionData::DeletionMapOption deletionMapOption)
{
    assert(deletionMapOption != DMO_PRIVATE_NEED);
    mDeletionMapOption = deletionMapOption;
    mSegmentDirectory = segDir;
    InitPartitionMeta(segDir->GetRootDirectory());
    if (mDeletionMapOption != DeletionMapOption::DMO_NO_NEED) {
        mDeletionMapReader = CreateDeletionMapReader(mDeletionMapOption);
        mPartitionInfoHolder = CreatePartitionInfoHolder();
    }

    SegmentDirectoryPtr subSegDir = segDir->GetSubSegmentDirectory();
    if (subSegDir) {
        mSubPartitionData.reset(new OnDiskPartitionData(mPluginManager));
        mSubPartitionData->Open(subSegDir, deletionMapOption);
        if (mPartitionInfoHolder) {
            mPartitionInfoHolder->SetSubPartitionInfoHolder(mSubPartitionData->GetPartitionInfoHolder());
        }
    }
}

PartitionInfoHolderPtr OnDiskPartitionData::GetPartitionInfoHolder() const { return mPartitionInfoHolder; }

void OnDiskPartitionData::InitPartitionMeta(const DirectoryPtr& directory)
{
    assert(directory);
    if (directory->IsExist(INDEX_PARTITION_META_FILE_NAME)) {
        mPartitionMeta.Load(directory);
    }
}

index_base::Version OnDiskPartitionData::GetVersion() const { return mSegmentDirectory->GetVersion(); }

index_base::Version OnDiskPartitionData::GetOnDiskVersion() const { return mSegmentDirectory->GetOnDiskVersion(); }

const DirectoryPtr& OnDiskPartitionData::GetRootDirectory() const { return mSegmentDirectory->GetRootDirectory(); }

SegmentData OnDiskPartitionData::GetSegmentData(segmentid_t segId) const
{
    return mSegmentDirectory->GetSegmentData(segId);
}

OnDiskPartitionData* OnDiskPartitionData::Clone() { return new OnDiskPartitionData(*this); }

DeletionMapReaderPtr OnDiskPartitionData::CreateDeletionMapReader(OnDiskPartitionData::DeletionMapOption option)
{
    DeletionMapReaderPtr deletionMapReader(new DeletionMapReader(option == OnDiskPartitionData::DMO_SHARED_NEED));
    deletionMapReader->Open(this);
    return deletionMapReader;
}

PartitionInfoHolderPtr OnDiskPartitionData::CreatePartitionInfoHolder()
{
    PartitionInfoHolderPtr partitionInfoHolder(new PartitionInfoHolder);
    partitionInfoHolder->Init(mSegmentDirectory->GetVersion(), mPartitionMeta, mSegmentDirectory->GetSegmentDatas(),
                              std::vector<InMemorySegmentPtr>(), GetDeletionMapReader());
    return partitionInfoHolder;
}

PartitionInfoPtr OnDiskPartitionData::GetPartitionInfo() const
{
    auto holder = GetPartitionInfoHolder();
    if (holder) {
        return holder->GetPartitionInfo();
    }
    return PartitionInfoPtr();
}

const TemperatureDocInfoPtr OnDiskPartitionData::GetTemperatureDocInfo() const
{
    auto partitionInfo = GetPartitionInfo();
    if (partitionInfo) {
        return partitionInfo->GetTemperatureDocInfo();
    }
    return TemperatureDocInfoPtr();
}

PartitionMeta OnDiskPartitionData::GetPartitionMeta() const
{
    if (mSegmentDirectory->IsSubSegmentDirectory()) {
        return PartitionMeta();
    }
    return mPartitionMeta;
}

const IndexFormatVersion& OnDiskPartitionData::GetIndexFormatVersion() const
{
    assert(mSegmentDirectory);
    return mSegmentDirectory->GetIndexFormatVersion();
}

uint32_t OnDiskPartitionData::GetIndexShardingColumnNum(const IndexPartitionOptions& options) const
{
    // TODO: online build support sharding column
    if (options.IsOnline()) {
        return 1;
    }

    uint32_t inSegColumnNum = SegmentInfo::INVALID_SHARDING_COUNT;
    for (Iterator iter = Begin(); iter != End(); iter++) {
        const std::shared_ptr<const SegmentInfo>& segInfo = (*iter).GetSegmentInfo();
        if (inSegColumnNum != SegmentInfo::INVALID_SHARDING_COUNT && inSegColumnNum != segInfo->shardCount) {
            INDEXLIB_FATAL_ERROR(InconsistentState, "segments with different shardCount for version[%d]!",
                                 GetOnDiskVersion().GetVersionId());
        }
        inSegColumnNum = segInfo->shardCount;
    }

    uint32_t inVersionColumnNum = GetOnDiskVersion().GetLevelInfo().GetShardCount();
    if (inSegColumnNum != SegmentInfo::INVALID_SHARDING_COUNT && inSegColumnNum != inVersionColumnNum) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "inSegColumnNum [%u] not equal with inVersionColumnNum [%u]!",
                             inSegColumnNum, inVersionColumnNum);
    }
    return inVersionColumnNum;
}

segmentid_t OnDiskPartitionData::GetLastValidRtSegmentInLinkDirectory() const
{
    assert(mSegmentDirectory);
    return mSegmentDirectory->GetLastValidRtSegmentInLinkDirectory();
}

segmentid_t OnDiskPartitionData::GetLastSegmentId() const
{
    return (*(mSegmentDirectory->GetSegmentDatas().rbegin())).GetSegmentId();
}

bool OnDiskPartitionData::SwitchToLinkDirectoryForRtSegments(segmentid_t lastLinkRtSegId)
{
    assert(mSegmentDirectory);
    return mSegmentDirectory->SwitchToLinkDirectoryForRtSegments(lastLinkRtSegId);
}

string OnDiskPartitionData::GetLastLocator() const
{
    assert(mSegmentDirectory);
    return mSegmentDirectory->GetLastLocator();
}

PartitionSegmentIteratorPtr OnDiskPartitionData::CreateSegmentIterator()
{
    bool isOnline = true;
    if (DYNAMIC_POINTER_CAST(OfflineSegmentDirectory, mSegmentDirectory)) {
        isOnline = false;
    }
    PartitionSegmentIteratorPtr segIter(new PartitionSegmentIterator(isOnline));
    segIter->Init(mSegmentDirectory->GetSegmentDatas(), std::vector<InMemorySegmentPtr>(), InMemorySegmentPtr());
    return segIter;
}

SegmentDataVector::const_iterator OnDiskPartitionData::Begin() const
{
    return mSegmentDirectory->GetSegmentDatas().begin();
}

SegmentDataVector::const_iterator OnDiskPartitionData::End() const
{
    return mSegmentDirectory->GetSegmentDatas().end();
}

SegmentDataVector& OnDiskPartitionData::GetSegmentDatas() { return mSegmentDirectory->GetSegmentDatas(); }

// static method to create OnDiskPartitionData
OnDiskPartitionDataPtr OnDiskPartitionData::CreateOnDiskPartitionData(const file_system::IFileSystemPtr& fileSystem,
                                                                      index_base::Version version,
                                                                      const std::string& dir, bool hasSubSegment,
                                                                      bool needDeletionMap,
                                                                      const plugin::PluginManagerPtr& pluginManager)
{
    DirectoryPtr baseDir = Directory::Get(fileSystem);
    if (!dir.empty()) {
        baseDir = baseDir->GetDirectory(dir, true);
    }
    // use NULL: will not trigger offline recover, for merger
    SegmentDirectoryPtr segDir = SegmentDirectoryCreator::Create(baseDir, version, NULL, hasSubSegment);
    OnDiskPartitionDataPtr partitionData(new OnDiskPartitionData(pluginManager));
    partitionData->Open(segDir,
                        needDeletionMap ? OnDiskPartitionData::DMO_SHARED_NEED : OnDiskPartitionData::DMO_NO_NEED);
    return partitionData;
}

OnDiskPartitionDataPtr OnDiskPartitionData::CreateOnDiskPartitionData(const file_system::IFileSystemPtr& fileSystem,
                                                                      const config::IndexPartitionSchemaPtr& schema,
                                                                      index_base::Version version,
                                                                      const std::string& dir, bool needDeletionMap,
                                                                      const plugin::PluginManagerPtr& pluginManager)
{
    bool hasSubSegment = false;
    if (schema->GetSubIndexPartitionSchema()) {
        hasSubSegment = true;
    }
    return CreateOnDiskPartitionData(fileSystem, version, dir, hasSubSegment, needDeletionMap, pluginManager);
}

OnDiskPartitionDataPtr
OnDiskPartitionData::CreateOnDiskPartitionData(const std::vector<file_system::DirectoryPtr> srcDirs,
                                               const std::vector<index_base::Version>& versions, bool hasSubSegment,
                                               bool needDeletionMap, const plugin::PluginManagerPtr& pluginManager)
{
    SegmentDirectoryPtr segDir = SegmentDirectoryCreator::Create(srcDirs, versions, hasSubSegment);
    OnDiskPartitionDataPtr onDiskPartitionData(new OnDiskPartitionData(pluginManager));
    onDiskPartitionData->Open(segDir, needDeletionMap ? OnDiskPartitionData::DMO_SHARED_NEED
                                                      : OnDiskPartitionData::DMO_NO_NEED);
    return onDiskPartitionData;
}

OnDiskPartitionDataPtr
OnDiskPartitionData::TEST_CreateOnDiskPartitionData(const IndexPartitionOptions& options,
                                                    const std::vector<std::string>& roots, bool hasSubSegment,
                                                    bool needDeletionMap, const plugin::PluginManagerPtr& pluginManager)
{
    DirectoryVector directoryVec = TEST_CreateDirectoryVector(options, roots);
    SegmentDirectoryPtr segDir = SegmentDirectoryCreator::Create(directoryVec, hasSubSegment);
    OnDiskPartitionDataPtr onDiskPartitionData(new OnDiskPartitionData(pluginManager));
    onDiskPartitionData->Open(segDir, needDeletionMap ? OnDiskPartitionData::DMO_SHARED_NEED
                                                      : OnDiskPartitionData::DMO_NO_NEED);
    return onDiskPartitionData;
}

file_system::DirectoryVector OnDiskPartitionData::TEST_CreateDirectoryVector(const IndexPartitionOptions& options,
                                                                             const std::vector<std::string>& roots)
{
    // TODO:
    DirectoryVector directoryVec;
    for (size_t i = 0; i < roots.size(); ++i) {
        // simple file system
        util::PartitionMemoryQuotaControllerPtr quotaController =
            util::MemoryQuotaControllerCreator::CreatePartitionMemoryController();

        FileSystemOptions fileSystemOptions;
        fileSystemOptions.needFlush = false;
        fileSystemOptions.useCache = false;
        fileSystemOptions.memoryQuotaController = quotaController;
        fileSystemOptions.loadConfigList = options.GetOnlineConfig().loadConfigList;
        IFileSystemPtr fileSystem =
            FileSystemCreator::Create(/*name=*/"uninitialized-name",
                                      /*rootPath=*/roots[i], fileSystemOptions, std::shared_ptr<util::MetricProvider>(),
                                      /*isOverride=*/false)
                .GetOrThrow();
        THROW_IF_FS_ERROR(fileSystem->MountVersion(roots[i], -1, "", FSMT_READ_ONLY, nullptr), "mount version failed");

        DirectoryPtr rootDirectory = Directory::Get(fileSystem);
        directoryVec.push_back(rootDirectory);
    }
    return directoryVec;
}

OnDiskPartitionDataPtr OnDiskPartitionData::CreateRealtimePartitionData(const file_system::DirectoryPtr& directory,
                                                                        bool enableRecover,
                                                                        const plugin::PluginManagerPtr& pluginManager,
                                                                        bool needDeletionMap)
{
    index_base::RealtimeSegmentDirectoryPtr segDir(new index_base::RealtimeSegmentDirectory(enableRecover));
    OnDiskPartitionDataPtr ret(new OnDiskPartitionData(pluginManager));
    segDir->Init(directory);
    ret->Open(segDir, needDeletionMap ? OnDiskPartitionData::DMO_SHARED_NEED : OnDiskPartitionData::DMO_NO_NEED);
    return ret;
}

}} // namespace indexlib::partition
