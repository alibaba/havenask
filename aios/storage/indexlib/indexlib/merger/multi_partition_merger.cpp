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
#include "indexlib/merger/multi_partition_merger.h"

#include <vector>

#include "indexlib/config/configurator_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_plugin_resource.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/meta_cache_preloader.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/merger/multi_part_segment_directory.h"
#include "indexlib/merger/sorted_index_partition_merger.h"
#include "indexlib/plugin/index_plugin_loader.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/util/PathUtil.h"

using namespace std;

using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlib::util;
using namespace indexlib::plugin;
using namespace indexlib::index_base;
using namespace indexlib::index;
using namespace indexlib::file_system;

namespace indexlib { namespace merger {

IE_LOG_SETUP(merger, MultiPartitionMerger);

MultiPartitionMerger::MultiPartitionMerger(const IndexPartitionOptions& options, MetricProviderPtr metricProvider,
                                           const string& indexPluginPath, const CommonBranchHinterOption& branchOption)
    : mOptions(options)
    , mMetricProvider(metricProvider)
    , mIndexPluginPath(indexPluginPath)
    , mBranchOption(branchOption)
{
    assert(mBranchOption.branchId == 0);
}

MultiPartitionMerger::~MultiPartitionMerger() {}

void MultiPartitionMerger::GetFirstVersion(const file_system::DirectoryPtr& partDirectory, index_base::Version& version)
{
    VersionLoader::GetVersion(partDirectory, version, INVALID_VERSION);
    MetaCachePreloader::Load(partDirectory, version.GetVersionId());
}

vector<DirectoryPtr> MultiPartitionMerger::CreateMergeSrcDirs(const vector<string>& mergeSrcPaths,
                                                              versionid_t versionId, MetricProviderPtr metricProvider)
{
    FileSystemOptions fsOptions = FileSystemOptions::OfflineWithBlockCache(metricProvider);
    fsOptions.needFlush = false;
    std::vector<file_system::DirectoryPtr> mergeSrcDirs;
    for (const std::string& mergeSrcPath : mergeSrcPaths) {
        string fsName = mergeSrcPath;
        PathUtil::TrimLastDelim(fsName);
        fsName = "MultiPartitionMerger-src" + PathUtil::GetFileName(fsName);
        auto fs = FileSystemCreator::Create(fsName, mergeSrcPath, fsOptions).GetOrThrow();
        THROW_IF_FS_ERROR(fs->MountVersion(mergeSrcPath, versionId, "", FSMT_READ_ONLY, nullptr),
                          "mount version failed");
        FenceContextPtr fenceContext(FslibWrapper::CreateFenceContext(mergeSrcPath, mBranchOption.epochId));
        auto directory =
            Directory::ConstructByFenceContext(Directory::Get(fs), fenceContext, fs->GetFileSystemMetricsReporter());
        mergeSrcDirs.emplace_back(directory);
    }
    return mergeSrcDirs;
}

IndexPartitionMergerPtr MultiPartitionMerger::CreatePartitionMerger(const std::vector<DirectoryPtr>& mergeSrcDirs,
                                                                    const std::string& destPath)
{
    if (mergeSrcDirs.size() == 0) {
        INDEXLIB_FATAL_ERROR(BadParameter, "No partition to merge."
                                           "when merging multi-partition");
    }

    if (destPath == "") {
        INDEXLIB_FATAL_ERROR(BadParameter, "No destPath specified."
                                           "when merging multi-partition");
    }

    // TODO(LFS_CREATE)
    FileSystemOptions fsOptions = FileSystemOptions::OfflineWithBlockCache(mMetricProvider);
    fsOptions.needFlush = false;
    auto fs = FileSystemCreator::Create("MultiPartitionMerger-dest", destPath, fsOptions).GetOrThrow();
    auto destDirectory = Directory::ConstructByFenceContext(
        Directory::Get(fs), FenceContextPtr(FslibWrapper::CreateFenceContext(destPath, mBranchOption.epochId)),
        fs->GetFileSystemMetricsReporter());

    Version firstVersion;
    GetFirstVersion(mergeSrcDirs[0], firstVersion);
    mSchema =
        SchemaAdapter::LoadAndRewritePartitionSchema(mergeSrcDirs[0], mOptions, firstVersion.GetSchemaVersionId());
    if (mSchema == NULL) {
        INDEXLIB_FATAL_ERROR(BadParameter, "No schema file in [%s] when merging multi-partition",
                             mergeSrcDirs[0]->DebugString().c_str());
    }
    IE_LOG(INFO, "loadAndRewrite schema done");
    PartitionMeta partitionMeta = PartitionMeta::LoadPartitionMeta(mergeSrcDirs[0]);
    IE_LOG(INFO, "load partitionMeta done");

    vector<Version> versions;
    CheckSrcPath(mergeSrcDirs, mSchema, partitionMeta, versions);
    CreateDestPath(destDirectory, mSchema, partitionMeta);
    mSegmentDirectory.reset(new MultiPartSegmentDirectory(mergeSrcDirs, versions));
    // KV & KKV has no deletionMap
    bool needDeletionMap = mSchema->GetTableType() == tt_index;
    mSegmentDirectory->Init(mSchema->GetSubIndexPartitionSchema() != NULL, needDeletionMap);
    IE_LOG(INFO, "segDir init done");
    Version destVersion;
    if (!versions.empty()) {
        destVersion.SetFormatVersion(versions[0].GetFormatVersion());
    }
    // TODO: if exist, right?
    VersionLoader::GetVersion(destDirectory, destVersion, INVALID_VERSION);
    if (destVersion == index_base::Version(INVALID_VERSION)) {
        const indexlibv2::framework::LevelInfo& srcLevelInfo = mSegmentDirectory->GetVersion().GetLevelInfo();
        indexlibv2::framework::LevelInfo& levelInfo = destVersion.GetLevelInfo();
        levelInfo.Init(srcLevelInfo.GetTopology(), srcLevelInfo.GetLevelCount(), srcLevelInfo.GetShardCount());
    }
    // TODO: set min ts
    destVersion.SetTimestamp(mSegmentDirectory->GetVersion().GetTimestamp());
    destVersion.SetLocator(mSegmentDirectory->GetVersion().GetLocator());
    destVersion.SetFormatVersion(mSegmentDirectory->GetVersion().GetFormatVersion());
    IE_LOG(INFO, "prepare dest version done");

    DumpStrategyPtr dumpStrategy;
    dumpStrategy.reset(new DumpStrategy(destDirectory, destVersion));

    PluginManagerPtr pluginManager = IndexPluginLoader::Load(mIndexPluginPath, mSchema->GetIndexSchema(), mOptions);
    if (!pluginManager) {
        INDEXLIB_FATAL_ERROR(Runtime, "load index plugin failed for schema[%s]", mSchema->GetSchemaName().c_str());
    }
    PluginResourcePtr resource(
        new IndexPluginResource(mSchema, mOptions, util::CounterMapPtr(), partitionMeta, mIndexPluginPath));
    pluginManager->SetPluginResource(resource);
    IE_LOG(INFO, "prepare pluginManager done");

    if (partitionMeta.Size() > 0) {
        return IndexPartitionMergerPtr(new SortedIndexPartitionMerger(
            mSegmentDirectory, mSchema, mOptions, partitionMeta.GetSortDescriptions(), dumpStrategy, mMetricProvider,
            pluginManager, mBranchOption, PartitionRange()));
    } else {
        return IndexPartitionMergerPtr(new IndexPartitionMerger(mSegmentDirectory, mSchema, mOptions, dumpStrategy,
                                                                mMetricProvider, pluginManager, mBranchOption,
                                                                PartitionRange()));
    }
}

void MultiPartitionMerger::Merge(const vector<string>& mergeSrc, const string& destPath)
{
    vector<DirectoryPtr> mergeSrcDirs = CreateMergeSrcDirs(mergeSrc, INVALID_VERSION, mMetricProvider);
    IndexPartitionMergerPtr merger = CreatePartitionMerger(mergeSrcDirs, destPath);
    merger->Merge(true);
}

void MultiPartitionMerger::CreateDestPath(const file_system::DirectoryPtr& destDirectory,
                                          const IndexPartitionSchemaPtr& schema, const PartitionMeta& partMeta)
{
    string destSchemaPath = Version::GetSchemaFileName(schema->GetSchemaVersionId());
    string indexFormatPath = INDEX_FORMAT_VERSION_FILE_NAME;
    string partitionMetaPath = INDEX_PARTITION_META_FILE_NAME;
    if (destDirectory->IsExist(destSchemaPath)) {
        CheckSchema(destDirectory, schema);
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

void MultiPartitionMerger::CheckSrcPath(const vector<DirectoryPtr>& mergeDirs, const IndexPartitionSchemaPtr& schema,
                                        const PartitionMeta& partMeta, vector<Version>& versions)
{
    versions.resize(mergeDirs.size());
    for (size_t i = 0; i < mergeDirs.size(); ++i) {
        VersionLoader::GetVersion(mergeDirs[i], versions[i], INVALID_VERSION);
        MetaCachePreloader::Load(mergeDirs[i], versions[i].GetVersionId());
        CheckPartitionConsistence(mergeDirs[i], schema, partMeta);
    }
}

void MultiPartitionMerger::CheckPartitionConsistence(const file_system::DirectoryPtr& rootDirectory,
                                                     const IndexPartitionSchemaPtr& schema,
                                                     const PartitionMeta& partMeta)
{
    // check index format
    CheckIndexFormat(rootDirectory);

    // check schema
    CheckSchema(rootDirectory, schema);

    // check partition meta
    CheckPartitionMeta(rootDirectory, partMeta);
}

void MultiPartitionMerger::CheckSchema(const file_system::DirectoryPtr& rootDirectory,
                                       const IndexPartitionSchemaPtr& schema)
{
    IndexPartitionSchemaPtr pSchema =
        SchemaAdapter::LoadAndRewritePartitionSchema(rootDirectory, mOptions, schema->GetSchemaVersionId());
    schema->AssertEqual(*pSchema);
}

void MultiPartitionMerger::CheckIndexFormat(const file_system::DirectoryPtr& rootDirectory)
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

void MultiPartitionMerger::CheckPartitionMeta(const file_system::DirectoryPtr& rootDirectory,
                                              const PartitionMeta& partMeta)
{
    PartitionMeta partitionMeta = PartitionMeta::LoadPartitionMeta(rootDirectory);
    partitionMeta.AssertEqual(partMeta);
}
}} // namespace indexlib::merger
