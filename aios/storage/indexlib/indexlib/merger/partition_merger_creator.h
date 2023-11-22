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

#include <string>
#include <vector>

#include "indexlib/config/index_partition_options.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/index/util/reclaim_map.h"
#include "indexlib/index_base/common_branch_hinter_option.h"
#include "indexlib/index_base/index_meta/parallel_build_info.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/custom_partition_merger.h"
#include "indexlib/merger/dump_strategy.h"
#include "indexlib/merger/partition_merger.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"
#include "indexlib/util/metrics/MetricProvider.h"

namespace indexlib { namespace merger {

class PartitionMergerCreator
{
public:
    PartitionMergerCreator();
    ~PartitionMergerCreator();

public:
    static PartitionMerger* CreateSinglePartitionMerger(const std::string& rootPath,
                                                        const config::IndexPartitionOptions& options,
                                                        util::MetricProviderPtr metricProvider,
                                                        const std::string& indexPluginPath,
                                                        const PartitionRange& targetRange,
                                                        const index_base::CommonBranchHinterOption& branchOption);

    static PartitionMerger* CreateIncParallelPartitionMerger(const std::string& rootDir,
                                                             const index_base::ParallelBuildInfo& info,
                                                             const config::IndexPartitionOptions& options,
                                                             util::MetricProviderPtr metricProvider,
                                                             const std::string& indexPluginPath,
                                                             const PartitionRange& targetRange,
                                                             const index_base::CommonBranchHinterOption& branchOption);

    static PartitionMerger* CreateFullParallelPartitionMerger(const std::vector<std::string>& mergeSrc,
                                                              const std::string& destPath,
                                                              const config::IndexPartitionOptions& options,
                                                              util::MetricProviderPtr metricProvider,
                                                              const std::string& indexPluginPath,
                                                              const PartitionRange& targetRange,
                                                              const index_base::CommonBranchHinterOption& branchOption);

public:
    static PartitionMerger* TEST_CreateSinglePartitionMerger(const std::string& rootPath,
                                                             const config::IndexPartitionOptions& options,
                                                             util::MetricProviderPtr metricProvider,
                                                             const std::string& indexPluginPath);

    static PartitionMerger* TEST_CreateIncParallelPartitionMerger(const std::string& rootDir,
                                                                  const index_base::ParallelBuildInfo& info,
                                                                  const config::IndexPartitionOptions& options,
                                                                  util::MetricProviderPtr metricProvider,
                                                                  const std::string& indexPluginPath);

    static PartitionMerger* TEST_CreateFullParallelPartitionMerger(const std::vector<std::string>& mergeSrc,
                                                                   const std::string& destPath,
                                                                   const config::IndexPartitionOptions& options,
                                                                   util::MetricProviderPtr metricProvider,
                                                                   const std::string& indexPluginPath);

private:
    static CustomPartitionMerger*
    CreateCustomPartitionMerger(const config::IndexPartitionSchemaPtr& schema,
                                const config::IndexPartitionOptions& options, const SegmentDirectoryPtr& segDir,
                                const DumpStrategyPtr& dumpStrategy, util::MetricProviderPtr metricProvider,
                                const plugin::PluginManagerPtr& pluginManager, const PartitionRange& targetRange,
                                const index_base::CommonBranchHinterOption& branchOption);

    static void CreateDestPath(const file_system::DirectoryPtr& destDirectory,
                               const config::IndexPartitionSchemaPtr& schema,
                               const config::IndexPartitionOptions& options, const index_base::PartitionMeta& partMeta);

    static void CheckSrcPath(const std::vector<file_system::DirectoryPtr>& mergeSrc,
                             const config::IndexPartitionSchemaPtr& schema,
                             const config::IndexPartitionOptions& options, const index_base::PartitionMeta& partMeta,
                             std::vector<index_base::Version>& versions);

    static void CheckPartitionConsistence(const file_system::DirectoryPtr& rootDirectory,
                                          const config::IndexPartitionSchemaPtr& schema,
                                          const config::IndexPartitionOptions& options,
                                          const index_base::PartitionMeta& partMeta);

    static void CheckSchema(const file_system::DirectoryPtr& rootDirectory,
                            const config::IndexPartitionSchemaPtr& schema,
                            const config::IndexPartitionOptions& options);

    static void CheckIndexFormat(const file_system::DirectoryPtr& rootDirectory);

    static void CheckPartitionMeta(const file_system::DirectoryPtr& rootDirectory,
                                   const index_base::PartitionMeta& partMeta);
    static void SortSubPartitions(std::vector<file_system::DirectoryPtr>& mergeSrcVec,
                                  std::vector<index_base::Version>& versions);

    static schemaid_t GetSchemaId(const file_system::DirectoryPtr& rootDirectory,
                                  const index_base::ParallelBuildInfo& parallelInfo);

    static file_system::FileSystemOptions GetDefaultFileSystemOptions(util::MetricProviderPtr metricProvider);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionMergerCreator);
}} // namespace indexlib::merger
