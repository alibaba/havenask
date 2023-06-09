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
#ifndef __INDEXLIB_INC_PARALLEL_PARTITION_MERGER_H
#define __INDEXLIB_INC_PARALLEL_PARTITION_MERGER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/SortDescription.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index_base/index_meta/parallel_build_info.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/index_partition_merger.h"
#include "indexlib/merger/merge_meta.h"

namespace indexlib { namespace merger {

class IncParallelPartitionMerger : public IndexPartitionMerger
{
public:
    IncParallelPartitionMerger(const IndexPartitionMergerPtr& merger, const std::vector<std::string>& srcDirs,
                               const indexlibv2::config::SortDescriptions& sortDesc,
                               util::MetricProviderPtr metricProvider, const plugin::PluginManagerPtr& pluginManager,
                               const PartitionRange& targetRange = PartitionRange());

    ~IncParallelPartitionMerger();

public:
    index_base::Version GetMergedVersion() const override { return mMerger->GetMergedVersion(); }

private:
    bool hasOngongingModifyOperationsChange(const index_base::Version& version,
                                            const config::IndexPartitionOptions& options);

public:
    // separate mode.
    MergeMetaPtr CreateMergeMeta(bool optimize, uint32_t instanceCount, int64_t currentTs) override
    {
        return mMerger->CreateMergeMeta(optimize, instanceCount, currentTs);
    }
    void PrepareMerge(int64_t currentTs) override;
    void DoMerge(bool optimize, const MergeMetaPtr& mergeMeta, uint32_t instanceId) override
    {
        return mMerger->DoMerge(optimize, mergeMeta, instanceId);
    }

    MergeMetaPtr LoadMergeMeta(const std::string& mergeMetaPath, bool onlyLoadBasicInfo) override
    {
        return mMerger->LoadMergeMeta(mergeMetaPath, onlyLoadBasicInfo);
    }

    void EndMerge(const MergeMetaPtr& mergeMeta, versionid_t alignVersionId = INVALID_VERSION) override;

    file_system::DirectoryPtr GetMergeIndexRootDirectory() const override
    {
        return mMerger->GetMergeIndexRootDirectory();
    }
    std::string GetMergeIndexRoot() const override { return mMerger->GetMergeIndexRoot(); }
    std::string GetMergeMetaDir() const override { return mMerger->GetMergeMetaDir(); }
    std::string GetMergeCheckpointDir() const override { return mMerger->GetMergeCheckpointDir(); }
    config::IndexPartitionSchemaPtr GetSchema() const override { return mMerger->GetSchema(); }

    const config::IndexPartitionOptions& GetOptions() const override { return mMerger->GetOptions(); }
    util::CounterMapPtr GetCounterMap() override { return mMerger->GetCounterMap(); }

    void SetCheckpointRootDir(const std::string& checkpoint) override { mMerger->SetCheckpointRootDir(checkpoint); }

    MergeFileSystemPtr CreateMergeFileSystem(uint32_t instanceId, const MergeMetaPtr& mergeMeta) override
    {
        return mMerger->CreateMergeFileSystem(instanceId, mergeMeta);
    }
    index_base::BranchFS* TEST_GetBranchFs() const override { return mMerger->TEST_GetBranchFs(); }

private:
    IndexPartitionMergerPtr mMerger;
    std::vector<std::string> mSrcDirs;
    indexlibv2::config::SortDescriptions mSortDesc;
    util::MetricProviderPtr mMetricProvider;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IncParallelPartitionMerger);
}} // namespace indexlib::merger

#endif //__INDEXLIB_INC_PARALLEL_PARTITION_MERGER_H
