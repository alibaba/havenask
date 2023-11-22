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
#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/partition_info_holder.h"

DECLARE_REFERENCE_CLASS(plugin, PluginManager);
DECLARE_REFERENCE_CLASS(index, DeletionMapReader);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

namespace indexlib { namespace partition {

class OnDiskPartitionData;
DEFINE_SHARED_PTR(OnDiskPartitionData);

// used by reader
class OnDiskPartitionData : public index_base::PartitionData
{
public:
    OnDiskPartitionData(const plugin::PluginManagerPtr& pluginManager);
    OnDiskPartitionData(const OnDiskPartitionData& other);
    ~OnDiskPartitionData();

public:
    enum DeletionMapOption {
        DMO_NO_NEED = 0,
        DMO_SHARED_NEED = 1,
        DMO_PRIVATE_NEED = 2,
    };
    virtual void Open(const index_base::SegmentDirectoryPtr& segDir, DeletionMapOption deletionMapOption = DMO_NO_NEED);

    index_base::Version GetVersion() const override;
    index_base::Version GetOnDiskVersion() const override;
    index_base::PartitionMeta GetPartitionMeta() const override;
    const index_base::IndexFormatVersion& GetIndexFormatVersion() const override;
    const file_system::DirectoryPtr& GetRootDirectory() const override;

    Iterator Begin() const override;
    Iterator End() const override;
    segmentid_t GetLastSegmentId() const override;
    index_base::SegmentData GetSegmentData(segmentid_t segId) const override;
    index_base::SegmentDataVector& GetSegmentDatas();

    OnDiskPartitionData* Clone() override;
    index_base::PartitionDataPtr GetSubPartitionData() const override { return mSubPartitionData; }
    index::DeletionMapReaderPtr GetDeletionMapReader() const override;
    index::PartitionInfoPtr GetPartitionInfo() const override;
    const index_base::TemperatureDocInfoPtr GetTemperatureDocInfo() const override;
    const index_base::SegmentDirectoryPtr& GetSegmentDirectory() const override { return mSegmentDirectory; }

    uint32_t GetIndexShardingColumnNum(const config::IndexPartitionOptions& options) const override;

    segmentid_t GetLastValidRtSegmentInLinkDirectory() const override;
    bool SwitchToLinkDirectoryForRtSegments(segmentid_t lastLinkRtSegId) override;
    index_base::PartitionSegmentIteratorPtr CreateSegmentIterator() override;
    std::string GetLastLocator() const override;

    const plugin::PluginManagerPtr& GetPluginManager() const override { return mPluginManager; }

public:
    // by default:
    // use file system's root as partition root
    // use SegmentDirectory as default segment directory
    static OnDiskPartitionDataPtr
    CreateOnDiskPartitionData(const std::shared_ptr<file_system::IFileSystem>& fileSystem,
                              index_base::Version version = index_base::Version(INVALID_VERSIONID),
                              const std::string& dir = "", bool hasSubSegment = false, bool needDeletionMap = false,
                              const plugin::PluginManagerPtr& pluginManager = plugin::PluginManagerPtr());

    static OnDiskPartitionDataPtr CreateOnDiskPartitionData(
        const std::shared_ptr<file_system::IFileSystem>& fileSystem, const config::IndexPartitionSchemaPtr& schema,
        index_base::Version version = index_base::Version(INVALID_VERSIONID), const std::string& dir = "",
        bool needDeletionMap = false, const plugin::PluginManagerPtr& pluginManager = plugin::PluginManagerPtr());

    static OnDiskPartitionDataPtr
    CreateOnDiskPartitionData(const std::vector<file_system::DirectoryPtr> srcDirs,
                              const std::vector<index_base::Version>& versions, bool hasSubSegment,
                              bool needDeletionMap = false,
                              const plugin::PluginManagerPtr& pluginManager = plugin::PluginManagerPtr());

    static OnDiskPartitionDataPtr CreateRealtimePartitionData(const file_system::DirectoryPtr& directory,
                                                              bool enableRecover,
                                                              const plugin::PluginManagerPtr& pluginManager,
                                                              bool needDeletionMap = false);

    static OnDiskPartitionDataPtr
    TEST_CreateOnDiskPartitionData(const config::IndexPartitionOptions& options, const std::vector<std::string>& roots,
                                   bool hasSubSegment = false, bool needDeletionMap = false,
                                   const plugin::PluginManagerPtr& pluginManager = plugin::PluginManagerPtr());

private:
    static std::vector<file_system::DirectoryPtr>
    TEST_CreateDirectoryVector(const config::IndexPartitionOptions& options, const std::vector<std::string>& roots);

protected:
    void InitPartitionMeta(const file_system::DirectoryPtr& directory);
    virtual index::DeletionMapReaderPtr CreateDeletionMapReader(OnDiskPartitionData::DeletionMapOption option);
    virtual PartitionInfoHolderPtr CreatePartitionInfoHolder();
    PartitionInfoHolderPtr GetPartitionInfoHolder() const;

protected:
    index_base::SegmentDirectoryPtr mSegmentDirectory;
    index_base::PartitionMeta mPartitionMeta;
    OnDiskPartitionDataPtr mSubPartitionData;
    index::DeletionMapReaderPtr mDeletionMapReader;
    PartitionInfoHolderPtr mPartitionInfoHolder;
    plugin::PluginManagerPtr mPluginManager;
    OnDiskPartitionData::DeletionMapOption mDeletionMapOption;

private:
    friend class OnDiskPartitionDataTest;
    IE_LOG_DECLARE();
};
}} // namespace indexlib::partition
