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
#include "indexlib/merger/inc_parallel_partition_merger.h"

#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/index_base/branch_fs.h"
#include "indexlib/index_base/version_committer.h"
#include "indexlib/merger/custom_partition_merger.h"
#include "indexlib/merger/parallel_partition_data_merger.h"
#include "indexlib/merger/sorted_index_partition_merger.h"
#include "indexlib/plugin/index_plugin_loader.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/table/table_factory_wrapper.h"
#include "indexlib/table/table_plugin_loader.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"

using namespace std;
using namespace indexlib::common;
using namespace indexlib::index_base;
using namespace indexlib::file_system;
using namespace indexlib::util;
using namespace indexlib::table;
using namespace indexlib::plugin;
namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, IncParallelPartitionMerger);

IncParallelPartitionMerger::IncParallelPartitionMerger(const IndexPartitionMergerPtr& merger,
                                                       const vector<std::string>& srcDirs,
                                                       const indexlibv2::config::SortDescriptions& sortDesc,
                                                       MetricProviderPtr metricProvider,
                                                       const plugin::PluginManagerPtr& pluginManager,
                                                       const PartitionRange& targetRange)
    : mMerger(merger)
    , mSrcDirs(srcDirs)
    , mSortDesc(sortDesc)
    , mMetricProvider(metricProvider)

{
    IndexPartitionMerger::mPartitionRange = targetRange;
    mPluginManager = pluginManager;
}

IncParallelPartitionMerger::~IncParallelPartitionMerger() {}

bool IncParallelPartitionMerger::hasOngongingModifyOperationsChange(const index_base::Version& version,
                                                                    const config::IndexPartitionOptions& options)
{
    auto versionOpIds = version.GetOngoingModifyOperations();
    sort(versionOpIds.begin(), versionOpIds.end());
    auto targetOpIds = options.GetOngoingModifyOperationIds();
    sort(targetOpIds.begin(), targetOpIds.end());
    if (versionOpIds.size() != targetOpIds.size()) {
        return true;
    }
    for (size_t i = 0; i < versionOpIds.size(); i++) {
        if (versionOpIds[i] != targetOpIds[i]) {
            return true;
        }
    }
    return false;
}

void IncParallelPartitionMerger::PrepareMerge(int64_t currentTs)
{
    auto destDirectory = mMerger->GetMergeIndexRootDirectory();
    auto schema = mMerger->GetSchema();
    auto fileSystem = destDirectory->GetFileSystem();
    ParallelPartitionDataMerger dataMerger(destDirectory, mMerger->GetBranchOption().epochId, schema,
                                           mMerger->GetOptions());
    Version version = dataMerger.MergeSegmentData(mSrcDirs);
    auto options = mMerger->GetOptions();
    if (hasOngongingModifyOperationsChange(version, options)) {
        version.SetVersionId(version.GetVersionId() + 1);
        version.SyncSchemaVersionId(schema);
        version.SetOngoingModifyOperations(options.GetOngoingModifyOperationIds());
        version.SetDescriptions(options.GetVersionDescriptions());
        version.SetUpdateableSchemaStandards(options.GetUpdateableSchemaStandards());
        index_base::VersionCommitter committer(destDirectory, version, INVALID_KEEP_VERSION_COUNT);
        committer.Commit();
    }
    SegmentDirectoryPtr segDir(new SegmentDirectory(destDirectory, version));
    bool needDeletionMap = schema->GetTableType() == tt_index;
    bool hasSub = false;
    if (schema->GetSubIndexPartitionSchema()) {
        hasSub = true;
    }
    segDir->Init(hasSub, needDeletionMap);
    if (schema->GetTableType() == tt_customized) {
        TableFactoryWrapperPtr tableFactory(new TableFactoryWrapper(schema, options, mPluginManager));
        auto targetRange = mMerger->GetPartitionRange();
        if (!tableFactory->Init()) {
            INDEXLIB_FATAL_ERROR(Runtime, "init table factory failed for schema[%s]", schema->GetSchemaName().c_str());
        }
        mMerger.reset(new CustomPartitionMerger(segDir, schema, options, DumpStrategyPtr(), mMetricProvider,
                                                tableFactory, mMerger->GetBranchOption(), targetRange));
    } else {
        if (mSortDesc.size() > 0) {
            mMerger.reset(new SortedIndexPartitionMerger(segDir, schema, options, mSortDesc, DumpStrategyPtr(),
                                                         mMetricProvider, mPluginManager, mMerger->GetBranchOption(),
                                                         PartitionRange()));
        } else {
            mMerger.reset(new IndexPartitionMerger(segDir, schema, options, DumpStrategyPtr(), mMetricProvider,
                                                   mPluginManager, mMerger->GetBranchOption(), PartitionRange()));
        }
    }
    mMerger->PrepareMerge(currentTs);
}

void IncParallelPartitionMerger::EndMerge(const MergeMetaPtr& mergeMeta, versionid_t alignVersionId)
{
    auto destDirectory = mMerger->GetMergeIndexRootDirectory();
    auto schema = mMerger->GetSchema();
    ParallelPartitionDataMerger dataMerger(destDirectory, mMerger->GetBranchOption().epochId, schema,
                                           mMerger->GetOptions());
    dataMerger.RemoveParallBuildDirectory();

    // string destPath = mMerger->GetMergeIndexRoot();
    // BranchFS::Create(destPath)->AbandonBranch(ParallelBuildInfo::IsParallelBuildPath);

    mMerger->EndMerge(mergeMeta, alignVersionId);
}
}} // namespace indexlib::merger
