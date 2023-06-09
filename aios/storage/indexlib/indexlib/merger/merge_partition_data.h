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
#ifndef __INDEXLIB_MERGE_PARTITION_DATA_H
#define __INDEXLIB_MERGE_PARTITION_DATA_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(plugin, PluginManager);

namespace indexlib { namespace merger {

class MergePartitionData;
DEFINE_SHARED_PTR(MergePartitionData);

// used by reader
class MergePartitionData : public index_base::PartitionData
{
public:
    MergePartitionData(const plugin::PluginManagerPtr& pluginManager);
    MergePartitionData(const MergePartitionData& other);
    ~MergePartitionData();

public:
    virtual void Open(const index_base::SegmentDirectoryPtr& segDir);

    index_base::Version GetVersion() const override;
    index_base::Version GetOnDiskVersion() const override;
    index_base::PartitionMeta GetPartitionMeta() const override;
    const index_base::IndexFormatVersion& GetIndexFormatVersion() const override;
    const file_system::DirectoryPtr& GetRootDirectory() const override;

    index_base::PartitionData::Iterator Begin() const override { return mSegmentDirectory->GetSegmentDatas().begin(); }

    index_base::PartitionData::Iterator End() const override { return mSegmentDirectory->GetSegmentDatas().end(); }

    segmentid_t GetLastSegmentId() const override;
    index_base::SegmentData GetSegmentData(segmentid_t segId) const override;
    MergePartitionData* Clone() override;
    index_base::PartitionDataPtr GetSubPartitionData() const override { return mSubPartitionData; }

    const index_base::SegmentDirectoryPtr& GetSegmentDirectory() const override { return mSegmentDirectory; }

    uint32_t GetIndexShardingColumnNum(const config::IndexPartitionOptions& options) const override;

    index_base::PartitionSegmentIteratorPtr CreateSegmentIterator() override;
    std::string GetLastLocator() const override;

    const plugin::PluginManagerPtr& GetPluginManager() const override { return mPluginManager; }

    // unsupported
    index::DeletionMapReaderPtr GetDeletionMapReader() const override;
    index::PartitionInfoPtr GetPartitionInfo() const override;
    segmentid_t GetLastValidRtSegmentInLinkDirectory() const override;
    bool SwitchToLinkDirectoryForRtSegments(segmentid_t lastLinkRtSegId) override;
    const index_base::TemperatureDocInfoPtr GetTemperatureDocInfo() const override;

protected:
    MergePartitionDataPtr CreatePartitionData();
    void InitPartitionMeta(const file_system::DirectoryPtr& directory);

protected:
    index_base::SegmentDirectoryPtr mSegmentDirectory;
    index_base::PartitionMeta mPartitionMeta;
    MergePartitionDataPtr mSubPartitionData;
    plugin::PluginManagerPtr mPluginManager;

private:
    friend class MergePartitionDataTest;
    IE_LOG_DECLARE();
};
}} // namespace indexlib::merger

#endif //__INDEXLIB_MERGE_PARTITION_DATA_H
