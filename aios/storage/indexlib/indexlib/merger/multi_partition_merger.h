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

#include <algorithm>
#include <string>
#include <vector>

#include "indexlib/base/Types.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index/util/reclaim_map.h"
#include "indexlib/index_base/common_branch_hinter_option.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/index_partition_merger.h"
#include "indexlib/merger/multi_part_segment_directory.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"
#include "indexlib/util/metrics/MetricProvider.h"

namespace indexlib { namespace merger {
class MultiPartitionMerger
{
public:
    typedef std::vector<std::string> StringVec;

public:
    MultiPartitionMerger(const config::IndexPartitionOptions& options, util::MetricProviderPtr metricProvider,
                         const std::string& indexPluginPath, const index_base::CommonBranchHinterOption& branchOption);

    virtual ~MultiPartitionMerger();

public:
    /**
     * Merge multi partition index.
     *
     * @param mergeSrc, the file system where the index partitions to merge
     * are stored.
     * @param mergeDest, the file system dirctory where the index partition
     * to be stored.
     *
     * @exception throw BadParameterException or FileIOException if failed.
     */
    void Merge(const std::vector<std::string>& mergeSrc, const std::string& mergeDest);

protected:
    std::vector<file_system::DirectoryPtr> CreateMergeSrcDirs(const std::vector<std::string>& mergeSrcPaths,
                                                              versionid_t versionId,
                                                              util::MetricProviderPtr metricProvider);

    IndexPartitionMergerPtr CreatePartitionMerger(const std::vector<file_system::DirectoryPtr>& mergeSrcDirs,
                                                  const std::string& mergeDest);

    virtual void GetFirstVersion(const file_system::DirectoryPtr& partDirectory, index_base::Version& version);

    void CreateDestPath(const file_system::DirectoryPtr& destDirectory, const config::IndexPartitionSchemaPtr& schema,
                        const index_base::PartitionMeta& partMeta);

    virtual void CheckSrcPath(const std::vector<file_system::DirectoryPtr>& mergeDirs,
                              const config::IndexPartitionSchemaPtr& schema, const index_base::PartitionMeta& partMeta,
                              std::vector<index_base::Version>& versions);
    void CheckPartitionConsistence(const file_system::DirectoryPtr& rootDirectory,
                                   const config::IndexPartitionSchemaPtr& schema,
                                   const index_base::PartitionMeta& partMeta);

    void CheckSchema(const file_system::DirectoryPtr& rootDirectory, const config::IndexPartitionSchemaPtr& schema);

    void CheckIndexFormat(const file_system::DirectoryPtr& rootDirectory);

    void CheckPartitionMeta(const file_system::DirectoryPtr& rootDirectory, const index_base::PartitionMeta& partMeta);

protected:
    config::IndexPartitionOptions mOptions;
    util::MetricProviderPtr mMetricProvider;
    std::string mIndexPluginPath;
    index_base::CommonBranchHinterOption mBranchOption;
    MultiPartSegmentDirectoryPtr mSegmentDirectory;
    indexlib::config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiPartitionMerger);
}} // namespace indexlib::merger
