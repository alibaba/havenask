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
#include "indexlib/merger/partition_merger_creator.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <ext/alloc_traits.h>
#include <memory>
#include <stdint.h>
#include <utility>

#include "alog/Logger.h"
#include "indexlib/base/Constant.h"
#include "indexlib/config/SortDescription.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/load_config_list.h"
#include "indexlib/config/updateable_schema_standards.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/FileSystemMetricsReporter.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/fslib/FenceContext.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/load_config/LoadStrategy.h"
#include "indexlib/framework/LevelInfo.h"
#include "indexlib/index/attribute/Constant.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_plugin_resource.h"
#include "indexlib/index_base/branch_fs.h"
#include "indexlib/index_base/common_branch_hinter.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/index_base/index_meta/segment_temperature_meta.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/patch/patch_file_info.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/merger/inc_parallel_partition_merger.h"
#include "indexlib/merger/index_partition_merger.h"
#include "indexlib/merger/merger_branch_hinter.h"
#include "indexlib/merger/multi_part_segment_directory.h"
#include "indexlib/merger/parallel_partition_data_merger.h"
#include "indexlib/merger/segment_directory.h"
#include "indexlib/merger/sorted_index_partition_merger.h"
#include "indexlib/plugin/index_plugin_loader.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/table/merge_task_description.h"
#include "indexlib/table/table_factory_wrapper.h"
#include "indexlib/table/table_plugin_loader.h"
#include "indexlib/util/ErrorLogCollector.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace indexlib::config;
using namespace indexlib::plugin;
using namespace indexlib::file_system;
using namespace indexlib::util;
using namespace indexlib::index_base;
using namespace indexlib::index;
using namespace indexlib::table;
using namespace indexlib::file_system;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, PartitionMergerCreator);

PartitionMergerCreator::PartitionMergerCreator() {}

PartitionMergerCreator::~PartitionMergerCreator() {}

PartitionMerger* PartitionMergerCreator::CreateSinglePartitionMerger(
    const std::string& rootPath, const IndexPartitionOptions& options, MetricProviderPtr metricProvider,
    const string& indexPluginPath, const PartitionRange& targetRange, const CommonBranchHinterOption& branchOption)
{
    unique_ptr<BranchFS> fs;
    fs = BranchFS::Create(rootPath);
    fs->Init(metricProvider, GetDefaultFileSystemOptions(metricProvider));

    {
        // this is for backup of full build without parallel, full build output path may be the branch path,
        // but output path of full merge must be the root path, so we need mount the markedBranch of build output.
        string markedBranch = BranchFS::GetDefaultBranch(rootPath, nullptr);
        if (!markedBranch.empty()) {
            fs->MountBranch(rootPath, markedBranch, INVALID_VERSIONID, FSMT_READ_ONLY);
        }
    }

    DirectoryPtr rootDirectory = Directory::ConstructByFenceContext(
        fs->GetRootDirectory(), FenceContextPtr(FslibWrapper::CreateFenceContext(rootPath, branchOption.epochId)),
        fs->GetFileSystem()->GetFileSystemMetricsReporter());
    Version version;

    VersionLoader::GetVersion(rootDirectory, version, INVALID_VERSIONID);
    IndexPartitionSchemaPtr schema = SchemaAdapter::LoadSchema(rootDirectory, version.GetSchemaVersionId());
    if (!schema) {
        ERROR_COLLECTOR_LOG(ERROR, "load schema failed");
        return NULL;
    }

    IndexPartitionOptions optionsRewrite = options;
    if (optionsRewrite.GetUpdateableSchemaStandards().IsEmpty() && !version.GetUpdateableSchemaStandards().IsEmpty()) {
        optionsRewrite.SetUpdateableSchemaStandards(version.GetUpdateableSchemaStandards());
    }

    if (schema->GetTableType() == tt_customized) {
        merger::SegmentDirectoryPtr segDir(new SegmentDirectory(rootDirectory, version));
        segDir->Init(false, false);
        PluginManagerPtr pluginManager = TablePluginLoader::Load(indexPluginPath, schema, optionsRewrite);
        return CreateCustomPartitionMerger(schema, optionsRewrite, segDir, DumpStrategyPtr(), metricProvider,
                                           pluginManager, targetRange, branchOption);
    }

    schema = SchemaAdapter::RewritePartitionSchema(schema, rootDirectory, optionsRewrite);
    if (!schema) {
        ERROR_COLLECTOR_LOG(ERROR, "rewrite schema failed");
        return NULL;
    }

    merger::SegmentDirectoryPtr segDir(new SegmentDirectory(rootDirectory, version));
    bool hasSub = false;
    if (schema->GetSubIndexPartitionSchema()) {
        hasSub = true;
    }

    if (schema->GetTableType() == tt_index && !(optionsRewrite.GetOngoingModifyOperationIds().empty())) {
        for (auto& id : optionsRewrite.GetOngoingModifyOperationIds()) {
            schema->MarkOngoingModifyOperation(id);
            IE_LOG(INFO, "mark op [%d] ungoing", id);
        }
    }

    // KV & KKV has no deletionMap
    bool needDeletionMap = schema->GetTableType() == tt_index;
    segDir->Init(hasSub, needDeletionMap);

    PartitionMeta partMeta = PartitionMeta::LoadPartitionMeta(rootDirectory);
    const indexlibv2::config::SortDescriptions& sortDescs = partMeta.GetSortDescriptions();
    bool needSort = sortDescs.size() > 0;

    PluginManagerPtr pluginManager = IndexPluginLoader::Load(indexPluginPath, schema->GetIndexSchema(), optionsRewrite);
    if (!pluginManager) {
        IE_LOG(ERROR, "load index plugin failed for schema [%s]", schema->GetSchemaName().c_str());
        return NULL;
    }

    PluginResourcePtr pluginResource(new IndexPluginResource(schema, optionsRewrite, util::CounterMapPtr(), partMeta,
                                                             indexPluginPath, metricProvider));

    pluginManager->SetPluginResource(pluginResource);
    if (!needSort) {
        return new IndexPartitionMerger(segDir, schema, optionsRewrite, DumpStrategyPtr(), metricProvider,
                                        pluginManager, branchOption, targetRange);
    } else {
        return new SortedIndexPartitionMerger(segDir, schema, optionsRewrite, sortDescs, DumpStrategyPtr(),
                                              metricProvider, pluginManager, branchOption, targetRange);
    }
}

PartitionMerger* PartitionMergerCreator::CreateIncParallelPartitionMerger(
    const std::string& root, const ParallelBuildInfo& parallelInfo, const IndexPartitionOptions& options,
    MetricProviderPtr metricProvider, const string& indexPluginPath, const PartitionRange& targetRange,
    const CommonBranchHinterOption& branchOption)
{
    unique_ptr<BranchFS> fs;
    fs = BranchFS::Create(root);

    fs->Init(metricProvider, GetDefaultFileSystemOptions(metricProvider));
    FenceContextPtr fenceContext(FslibWrapper::CreateFenceContext(root, branchOption.epochId));
    DirectoryPtr rootDirectory = Directory::ConstructByFenceContext(
        fs->GetRootDirectory(), fenceContext, fs->GetFileSystem()->GetFileSystemMetricsReporter());

    Version version;
    VersionLoader::GetVersion(rootDirectory, version, INVALID_VERSIONID);
    schemaid_t schemaId = version.GetVersionId() != INVALID_VERSIONID ? version.GetSchemaVersionId()
                                                                      : GetSchemaId(rootDirectory, parallelInfo);
    IndexPartitionSchemaPtr schema = SchemaAdapter::LoadAndRewritePartitionSchema(rootDirectory, options, schemaId);
    if (!schema) {
        ERROR_COLLECTOR_LOG(ERROR, "load schema failed");
        return NULL;
    }
    if (version.GetVersionId() == INVALID_VERSIONID) {
        version.SyncSchemaVersionId(schema);
    }

    merger::SegmentDirectoryPtr segDir(new SegmentDirectory(rootDirectory, version));
    bool hasSub = false;
    if (schema->GetSubIndexPartitionSchema()) {
        hasSub = true;
    }

    if (schema->GetTableType() == tt_index && !(options.GetOngoingModifyOperationIds().empty())) {
        for (auto& id : options.GetOngoingModifyOperationIds()) {
            schema->MarkOngoingModifyOperation(id);
            IE_LOG(INFO, "mark op [%d] ungoing", id);
        }
    }

    // KV & KKV has no deletionMap
    bool needDeletionMap = schema->GetTableType() == tt_index;
    segDir->Init(hasSub, needDeletionMap);

    PartitionMeta partMeta = PartitionMeta::LoadPartitionMeta(rootDirectory);
    const indexlibv2::config::SortDescriptions& sortDescs = partMeta.GetSortDescriptions();
    bool needSort = sortDescs.size() > 0;

    PluginManagerPtr pluginManager;
    IndexPartitionMergerPtr merger;

    IndexPartitionOptions optionsRewrite = options;
    if (optionsRewrite.GetUpdateableSchemaStandards().IsEmpty() && !version.GetUpdateableSchemaStandards().IsEmpty()) {
        optionsRewrite.SetUpdateableSchemaStandards(version.GetUpdateableSchemaStandards());
    }

    if (schema->GetTableType() == tt_customized) {
        pluginManager = TablePluginLoader::Load(indexPluginPath, schema, optionsRewrite);
        merger.reset(CreateCustomPartitionMerger(schema, optionsRewrite, segDir, DumpStrategyPtr(), metricProvider,
                                                 pluginManager, targetRange, branchOption));
    } else {
        pluginManager = IndexPluginLoader::Load(indexPluginPath, schema->GetIndexSchema(), optionsRewrite);
        if (!pluginManager) {
            IE_LOG(ERROR, "load index plugin failed for schema [%s]", schema->GetSchemaName().c_str());
            return NULL;
        }
        PluginResourcePtr pluginResource(new IndexPluginResource(schema, optionsRewrite, util::CounterMapPtr(),
                                                                 partMeta, indexPluginPath, metricProvider));
        pluginManager->SetPluginResource(pluginResource);
        if (!needSort) {
            merger.reset(new IndexPartitionMerger(segDir, schema, optionsRewrite, DumpStrategyPtr(), metricProvider,
                                                  pluginManager, branchOption, targetRange));
        } else {
            merger.reset(new SortedIndexPartitionMerger(segDir, schema, optionsRewrite, sortDescs, DumpStrategyPtr(),
                                                        metricProvider, pluginManager, branchOption, targetRange));
        }
    }

    vector<string> mergeSrcs = ParallelPartitionDataMerger::GetParallelInstancePaths(root, parallelInfo);
    return new IncParallelPartitionMerger(merger, mergeSrcs, sortDescs, metricProvider, pluginManager, targetRange);
}

PartitionMerger* PartitionMergerCreator::CreateFullParallelPartitionMerger(
    const vector<string>& mergeSrc, const std::string& destPath, const IndexPartitionOptions& options,
    MetricProviderPtr metricProvider, const string& indexPluginPath, const PartitionRange& targetRange,
    const CommonBranchHinterOption& branchOption)
{
    if (mergeSrc.size() == 0) {
        INDEXLIB_FATAL_ERROR(BadParameter, "No partition to merge."
                                           "when merging multi-partition");
    }

    if (destPath == "") {
        INDEXLIB_FATAL_ERROR(BadParameter, "No destPath specified."
                                           "when merging multi-partition");
    }
    // TODO(LFS_CREATE)
    std::vector<DirectoryPtr> mergeDirs;
    MergerBranchHinter hinter(branchOption);
    for (const auto& secondPath : mergeSrc) {
        string branchName;
        auto branchDirectory = BranchFS::GetDirectoryFromDefaultBranch(
            secondPath, GetDefaultFileSystemOptions(metricProvider), &hinter, &branchName);
        if (!hinter.CanOperateBranch(branchName)) {
            INDEXLIB_FATAL_ERROR(Runtime, "old worker with epochId [%s] cannot move new branch [%s] with epochId [%s]",
                                 hinter.GetOption().epochId.c_str(), branchName.c_str(),
                                 CommonBranchHinter::ExtractEpochFromBranch(branchName).c_str());
        }
        FileSystemMetricsReporter* reporter = branchDirectory->GetFileSystem() != nullptr
                                                  ? branchDirectory->GetFileSystem()->GetFileSystemMetricsReporter()
                                                  : nullptr;
        mergeDirs.push_back(Directory::ConstructByFenceContext(
            branchDirectory, FenceContextPtr(FslibWrapper::CreateFenceContext(secondPath, branchOption.epochId)),
            reporter));
    }
    unique_ptr<BranchFS> fs;
    fs = BranchFS::Create(destPath);
    fs->Init(metricProvider, GetDefaultFileSystemOptions(metricProvider));
    DirectoryPtr destDirectory = Directory::ConstructByFenceContext(
        fs->GetRootDirectory(), FenceContextPtr(FslibWrapper::CreateFenceContext(destPath, branchOption.epochId)),
        fs->GetFileSystem()->GetFileSystemMetricsReporter());

    Version firstVersion;
    VersionLoader::GetVersion(mergeDirs[0], firstVersion, INVALID_VERSIONID);
    IndexPartitionSchemaPtr schema = SchemaAdapter::LoadSchema(mergeDirs[0], firstVersion.GetSchemaVersionId());
    if (schema == NULL) {
        INDEXLIB_FATAL_ERROR(BadParameter,
                             "No schema file in [%s]."
                             "when merging multi-partition",
                             mergeSrc[0].c_str());
    }

    if (schema->GetTableType() != tt_customized) {
        schema = SchemaAdapter::RewritePartitionSchema(schema, mergeDirs[0], options);
        if (!schema) {
            ERROR_COLLECTOR_LOG(ERROR, "rewrite schema failed");
            return NULL;
        }
    }
    CheckIndexFormat(mergeDirs[0]);
    if (schema->GetTableType() == tt_index && !(options.GetOngoingModifyOperationIds().empty())) {
        for (auto& id : options.GetOngoingModifyOperationIds()) {
            schema->MarkOngoingModifyOperation(id);
            IE_LOG(INFO, "mark op [%d] ungoing", id);
        }
    }
    IE_LOG(INFO, "loadAndRewrite schema done");
    PartitionMeta partitionMeta = PartitionMeta::LoadPartitionMeta(mergeDirs[0]);
    IE_LOG(INFO, "load partitionMeta done");

    vector<file_system::DirectoryPtr> mergeSrcVec(mergeDirs.begin(), mergeDirs.end());
    vector<Version> versions;
    CheckSrcPath(mergeSrcVec, schema, options, partitionMeta, versions);
    SortSubPartitions(mergeSrcVec, versions);
    IE_LOG(INFO, "sort SubPartitions done");
    CreateDestPath(destDirectory, schema, options, partitionMeta);

    MultiPartSegmentDirectoryPtr multiSegDir(new MultiPartSegmentDirectory(mergeSrcVec, versions));
    if (schema->GetTableType() == tt_customized) {
        multiSegDir->Init(false, false);
    } else {
        // KV & KKV has no deletionMap
        bool needDeletionMap = schema->GetTableType() == tt_index;
        multiSegDir->Init(schema->GetSubIndexPartitionSchema() != NULL, needDeletionMap);
    }
    IE_LOG(INFO, "segDir init done");
    Version destVersion;
    // TODO: if exist, right?
    VersionLoader::GetVersion(destDirectory, destVersion, INVALID_VERSIONID);
    if (destVersion == index_base::Version(INVALID_VERSIONID)) {
        const indexlibv2::framework::LevelInfo& srcLevelInfo = multiSegDir->GetVersion().GetLevelInfo();
        indexlibv2::framework::LevelInfo& levelInfo = destVersion.GetLevelInfo();
        levelInfo.Init(srcLevelInfo.GetTopology(), srcLevelInfo.GetLevelCount(), srcLevelInfo.GetShardCount());
    }
    // TODO: set min ts
    destVersion.SetTimestamp(multiSegDir->GetVersion().GetTimestamp());
    destVersion.SetLocator(multiSegDir->GetVersion().GetLocator());
    destVersion.SetFormatVersion(multiSegDir->GetVersion().GetFormatVersion());
    IE_LOG(INFO, "prepare dest version done");

    DumpStrategyPtr dumpStrategy;
    dumpStrategy.reset(new DumpStrategy(destDirectory, destVersion));

    if (schema->GetTableType() == tt_customized) {
        PluginManagerPtr pluginManager = TablePluginLoader::Load(indexPluginPath, schema, options);
        return CreateCustomPartitionMerger(schema, options, multiSegDir, dumpStrategy, metricProvider, pluginManager,
                                           targetRange, branchOption);
    }
    PluginManagerPtr pluginManager = IndexPluginLoader::Load(indexPluginPath, schema->GetIndexSchema(), options);
    if (!pluginManager) {
        INDEXLIB_FATAL_ERROR(Runtime, "load index plugin failed for schema[%s]", schema->GetSchemaName().c_str());
    }
    PluginResourcePtr resource(new IndexPluginResource(schema, options, util::CounterMapPtr(), partitionMeta,
                                                       indexPluginPath, metricProvider));
    pluginManager->SetPluginResource(resource);
    IE_LOG(INFO, "prepare pluginManager done");

    if (partitionMeta.Size() > 0) {
        return new SortedIndexPartitionMerger(multiSegDir, schema, options, partitionMeta.GetSortDescriptions(),
                                              dumpStrategy, metricProvider, pluginManager, branchOption, targetRange);
    } else {
        return new IndexPartitionMerger(multiSegDir, schema, options, dumpStrategy, metricProvider, pluginManager,
                                        branchOption, targetRange);
    }
}

CustomPartitionMerger* PartitionMergerCreator::CreateCustomPartitionMerger(
    const IndexPartitionSchemaPtr& schema, const IndexPartitionOptions& options,
    const merger::SegmentDirectoryPtr& segDir, const DumpStrategyPtr& dumpStrategy, MetricProviderPtr metricProvider,
    const PluginManagerPtr& pluginManager, const PartitionRange& targetRange,
    const CommonBranchHinterOption& branchOption)
{
    if (!pluginManager) {
        IE_LOG(ERROR, "load table plugin for schema [%s]", schema->GetSchemaName().c_str());
        return NULL;
    }
    auto indexPluginPath = pluginManager->GetModuleRootPath();

    PluginResourcePtr resource(new PluginResource(schema, options, util::CounterMapPtr(), indexPluginPath));
    pluginManager->SetPluginResource(resource);

    TableFactoryWrapperPtr tableFactory(new TableFactoryWrapper(schema, options, pluginManager));
    if (!tableFactory->Init()) {
        IE_LOG(ERROR, "init table factory failed for schema[%s]", schema->GetSchemaName().c_str());
        return NULL;
    }
    return new CustomPartitionMerger(segDir, schema, options, dumpStrategy, metricProvider, tableFactory, branchOption,
                                     targetRange);
}

void PartitionMergerCreator::CreateDestPath(const file_system::DirectoryPtr& destDirectory,
                                            const IndexPartitionSchemaPtr& schema, const IndexPartitionOptions& options,
                                            const PartitionMeta& partMeta)
{
    string destSchemaPath = Version::GetSchemaFileName(schema->GetSchemaVersionId());
    string indexFormatPath = INDEX_FORMAT_VERSION_FILE_NAME;
    string partitionMetaPath = INDEX_PARTITION_META_FILE_NAME;

    if (destDirectory->IsExist(destSchemaPath)) {
        CheckSchema(destDirectory, schema, options);
    } else {
        SchemaAdapter::StoreSchema(destDirectory, schema);
    }

    if (destDirectory->IsExist(indexFormatPath)) {
        CheckIndexFormat(destDirectory);
    } else {
        IndexFormatVersion::StoreBinaryVersion(destDirectory);
    }

    if (destDirectory->IsExist(partitionMetaPath)) {
        CheckPartitionMeta(destDirectory, partMeta);
    } else {
        partMeta.Store(destDirectory);
    }
}

void PartitionMergerCreator::CheckSrcPath(const vector<file_system::DirectoryPtr>& mergeDirs,
                                          const IndexPartitionSchemaPtr& schema, const IndexPartitionOptions& options,
                                          const PartitionMeta& partMeta, vector<Version>& versions)
{
    versions.resize(mergeDirs.size());
    for (size_t i = 0; i < mergeDirs.size(); ++i) {
        if (i > 0) {
            CheckPartitionConsistence(mergeDirs[i], schema, options, partMeta);
        }
        VersionLoader::GetVersion(mergeDirs[i], versions[i], INVALID_VERSIONID);
    }
}

void PartitionMergerCreator::CheckPartitionConsistence(const file_system::DirectoryPtr& rootDirectory,
                                                       const IndexPartitionSchemaPtr& schema,
                                                       const IndexPartitionOptions& options,
                                                       const PartitionMeta& partMeta)
{
    // check index format
    CheckIndexFormat(rootDirectory);

    // check schema
    CheckSchema(rootDirectory, schema, options);

    // check partition meta
    if (schema->GetTableType() != tt_customized) {
        CheckPartitionMeta(rootDirectory, partMeta);
    }
}

void PartitionMergerCreator::CheckSchema(const file_system::DirectoryPtr& rootDirectory,
                                         const IndexPartitionSchemaPtr& schema, const IndexPartitionOptions& options)
{
    IndexPartitionSchemaPtr pSchema;
    if (schema->GetTableType() == tt_customized) {
        pSchema = SchemaAdapter::LoadSchema(rootDirectory, schema->GetSchemaVersionId());
    } else {
        pSchema = SchemaAdapter::LoadAndRewritePartitionSchema(rootDirectory, options, schema->GetSchemaVersionId());
    }
    schema->AssertEqual(*pSchema);
}

void PartitionMergerCreator::CheckIndexFormat(const file_system::DirectoryPtr& rootDirectory)
{
    IndexFormatVersion onDiskFormatVersion;
    onDiskFormatVersion.Load(rootDirectory);
    IndexFormatVersion binaryFormatVersion(INDEX_FORMAT_VERSION);
    if (onDiskFormatVersion != binaryFormatVersion) {
        INDEXLIB_FATAL_ERROR(BadParameter,
                             "index format version in [%s]"
                             " not equal with binaryversion [%s]",
                             rootDirectory->DebugString().c_str(), INDEX_FORMAT_VERSION);
    }
}

void PartitionMergerCreator::CheckPartitionMeta(const file_system::DirectoryPtr& rootDirectory,
                                                const PartitionMeta& partMeta)
{
    PartitionMeta partitionMeta = PartitionMeta::LoadPartitionMeta(rootDirectory);
    partitionMeta.AssertEqual(partMeta);
}

void PartitionMergerCreator::SortSubPartitions(vector<file_system::DirectoryPtr>& mergeSrcVec,
                                               vector<Version>& versions)
{
    assert(mergeSrcVec.size() == versions.size());
    typedef pair<size_t, Version> VersionInfo;
    typedef pair<file_system::DirectoryPtr, VersionInfo> SubPartitionInfo;
    typedef vector<SubPartitionInfo> SubPartitionInfos;

    SubPartitionInfos partInfos;
    for (size_t i = 0; i < mergeSrcVec.size(); i++) {
        IE_LOG(INFO, "src path [%s], ts [%ld]", mergeSrcVec[i]->DebugString().c_str(), versions[i].GetTimestamp());
        partInfos.push_back(make_pair(mergeSrcVec[i], make_pair(i, versions[i])));
    }
    stable_sort(partInfos.begin(), partInfos.end(),
                [](const pair<file_system::DirectoryPtr, pair<size_t, Version>>& lft,
                   const pair<file_system::DirectoryPtr, pair<size_t, Version>>& rht) -> bool {
                    int64_t lftTs = lft.second.second.GetTimestamp();
                    int64_t rhtTs = rht.second.second.GetTimestamp();
                    if (lftTs == rhtTs) {
                        return lft.second.first < rht.second.first;
                    }
                    if (lftTs == INVALID_TIMESTAMP) {
                        return true;
                    }
                    if (rhtTs == INVALID_TIMESTAMP) {
                        return false;
                    }
                    return lftTs > rhtTs;
                });

    mergeSrcVec.clear();
    versions.clear();
    for (size_t i = 0; i < partInfos.size(); i++) {
        auto partDir = partInfos[i].first;
        IE_LOG(INFO, "sequence [%lu], root [%s], version ts[%ld]", i, partDir->DebugString().c_str(),
               partInfos[i].second.second.GetTimestamp());
        mergeSrcVec.push_back(partInfos[i].first);
        versions.push_back(partInfos[i].second.second);
    }
}

schemaid_t PartitionMergerCreator::GetSchemaId(const file_system::DirectoryPtr& rootDirectory,
                                               const ParallelBuildInfo& parallelInfo)
{
    for (size_t i = 0; i < parallelInfo.parallelNum; i++) {
        ParallelBuildInfo info = parallelInfo;
        info.instanceId = i;

        Version instVersion;
        string branchName;
        auto instDir = BranchFS::GetDirectoryFromDefaultBranch(
            info.GetParallelInstancePath(rootDirectory->GetFileSystem()->GetOutputRootPath()),
            GetDefaultFileSystemOptions(nullptr), nullptr, &branchName);
        VersionLoader::GetVersion(instDir, instVersion, INVALID_VERSIONID);
        if (instVersion.GetVersionId() != INVALID_VERSIONID) {
            return instVersion.GetSchemaVersionId();
        }
    }
    assert(false);
    return DEFAULT_SCHEMAID;
}

file_system::FileSystemOptions PartitionMergerCreator::GetDefaultFileSystemOptions(MetricProviderPtr metricProvider)
{
    file_system::FileSystemOptions fsOptions = FileSystemOptions::OfflineWithBlockCache(metricProvider);
    fsOptions.enableAsyncFlush = false;
    return fsOptions;
}

PartitionMerger* PartitionMergerCreator::TEST_CreateSinglePartitionMerger(const std::string& rootPath,
                                                                          const IndexPartitionOptions& options,
                                                                          MetricProviderPtr metricProvider,
                                                                          const string& indexPluginPath)
{
    return CreateSinglePartitionMerger(rootPath, options, metricProvider, indexPluginPath, PartitionRange(),
                                       CommonBranchHinterOption::Test());
}

PartitionMerger* PartitionMergerCreator::TEST_CreateIncParallelPartitionMerger(
    const std::string& rootDir, const index_base::ParallelBuildInfo& info, const config::IndexPartitionOptions& options,
    util::MetricProviderPtr metricProvider, const std::string& indexPluginPath)
{
    return CreateIncParallelPartitionMerger(rootDir, info, options, metricProvider, indexPluginPath, PartitionRange(),
                                            CommonBranchHinterOption::Test());
}

PartitionMerger* PartitionMergerCreator::TEST_CreateFullParallelPartitionMerger(
    const std::vector<std::string>& mergeSrc, const std::string& destPath, const config::IndexPartitionOptions& options,
    util::MetricProviderPtr metricProvider, const std::string& indexPluginPath)
{
    return CreateFullParallelPartitionMerger(mergeSrc, destPath, options, metricProvider, indexPluginPath,
                                             PartitionRange(), CommonBranchHinterOption::Test());
}

}} // namespace indexlib::merger
