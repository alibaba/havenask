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
#include "indexlib/partition/remote_access/partition_resource_provider.h"

#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/MountOption.h"
#include "indexlib/index/normal/inverted_index/customized_index/index_plugin_resource.h"
#include "indexlib/index_base/index_meta/segment_file_list_wrapper.h"
#include "indexlib/index_base/patch/partition_patch_index_accessor.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/index_base/version_committer.h"
#include "indexlib/partition/index_partition_creator.h"
#include "indexlib/partition/index_partition_resource.h"
#include "indexlib/partition/offline_partition.h"
#include "indexlib/plugin/index_plugin_loader.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/TaskScheduler.h"
#include "indexlib/util/counter/CounterMap.h"

using namespace std;
using namespace fslib;

using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::util;
using namespace indexlib::plugin;
using namespace indexlib::common;
using namespace indexlib::index_base;
using namespace indexlib::file_system;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, PartitionResourceProvider);

PartitionResourceProvider::PartitionResourceProvider(const config::IndexPartitionOptions& options)
    : mTargetVersionId(INVALID_VERSION)
    , mOptions(options)
{
    int64_t minCacheSize = autil::EnvUtil::getEnv("INDEXLIB_PROVIDER_CACHE_SIZE_MB", 512);
    int64_t minBlockSize = autil::EnvUtil::getEnv("INDEXLIB_PROVIDER_BLOCK_SIZE_BYTE", 512 * 1024);
    minCacheSize *= (1024 * 1024);
    if (mOptions.GetOfflineConfig().readerConfig.indexCacheSize < minCacheSize) {
        mOptions.GetOfflineConfig().readerConfig.indexCacheSize = minCacheSize;
        IE_LOG(INFO, "adjust index cache size to [%lu] byte", minCacheSize);
    }
    if (mOptions.GetOfflineConfig().readerConfig.summaryCacheSize < minCacheSize) {
        IE_LOG(INFO, "adjust summary cache size to [%lu] byte", minCacheSize);
        mOptions.GetOfflineConfig().readerConfig.summaryCacheSize = minCacheSize;
    }
    if (mOptions.GetOfflineConfig().readerConfig.cacheBlockSize < minBlockSize) {
        IE_LOG(INFO, "adjust block size to [%lu] byte", minBlockSize);
        mOptions.GetOfflineConfig().readerConfig.cacheBlockSize = minBlockSize;
    }
    mOptions.SetIsOnline(false);
    mOptions.GetOfflineConfig().enableRecoverIndex = false;
    mOptions.GetBuildConfig(false).keepVersionCount = INVALID_KEEP_VERSION_COUNT;
    mPluginManagerPair.first = -1;
}

PartitionResourceProvider::~PartitionResourceProvider() {}

void PartitionResourceProvider::Sync()
{
    if (mDirectory != nullptr) {
        mDirectory->Sync(true /* wait finish */);
    }
}

bool PartitionResourceProvider::EnsurePartition(bool isSeeker) const
{
    if (mOfflinePartition) {
        if ((mCurrentOptions.GetOfflineConfig().readerConfig.readPreference == ReadPreference::RP_RANDOM_PREFER) !=
            isSeeker) {
            mDirectory.reset();
            mOfflinePartition.reset();
        }
    }
    if (!mOfflinePartition) {
        mCurrentOptions = mOptions;
        if (isSeeker) {
            mCurrentOptions.GetOfflineConfig().readerConfig.readPreference = RP_RANDOM_PREFER;
        } else {
            mCurrentOptions.GetOfflineConfig().readerConfig.readPreference = RP_SEQUENCE_PREFER;
        }
    }

    // if not legacy, partition is unable to find patch dir due to complex patch meta logic
    // TODO (yiping.typ) remove this hack and ugly @SetBranchNameLegacy
    IndexPartitionPtr IndexPartition = IndexPartitionCreator(0)
                                           .SetIndexPluginPath(mPluginPath)
                                           .SetPartitionName("PartitionResourceProvider")
                                           .SetBranchHinterOption(CommonBranchHinterOption::Legacy(0, ""))
                                           .CreateNormal(mCurrentOptions);
    OfflinePartitionPtr offlinePartition = DYNAMIC_POINTER_CAST(OfflinePartition, IndexPartition);
    if (offlinePartition->Open(mPath, "", IndexPartitionSchemaPtr(), mCurrentOptions, mTargetVersionId) !=
        IndexPartition::OS_OK) {
        IE_LOG(ERROR, "Open OfflinePartition fail!");
        return false;
    }

    mOfflinePartition = offlinePartition;
    mVersion = mOfflinePartition->GetPartitionData()->GetOnDiskVersion();
    mSchema = mOfflinePartition->GetSchema();

    mDirectory = offlinePartition->GetRootDirectory();
    if (mDirectory == nullptr) {
        IE_LOG(ERROR, "get directory failed, path[%s]", mPath.c_str());
        return false;
    }

    IE_LOG(INFO, "Init PartitionResourceProvider for path [%s] with version id [%d]", mPath.c_str(), mTargetVersionId);
    return true;
}

bool PartitionResourceProvider::Init(const string& indexPath, versionid_t targetVersionId, const string& pluginPath)
{
    mPath = PathUtil::NormalizePath(indexPath);
    mTargetVersionId = targetVersionId;
    mPluginPath = pluginPath;
    return EnsurePartition(false);
}

PartitionIteratorPtr PartitionResourceProvider::CreatePartitionIterator()
{
    if (!EnsurePartition(false)) {
        return PartitionIteratorPtr();
    }
    PartitionIteratorPtr iterator(new PartitionIterator);
    if (!iterator->Init(mOfflinePartition)) {
        IE_LOG(ERROR, "init partition iterator fail!");
        return PartitionIteratorPtr();
    }

    IE_LOG(INFO, "Create PartitionIterator for path [%s]", mPath.c_str());
    return iterator;
}

PartitionSeekerPtr PartitionResourceProvider::CreatePartitionSeeker()
{
    if (!EnsurePartition(true)) {
        return PartitionSeekerPtr();
    }
    PartitionSeekerPtr seeker(new PartitionSeeker);
    if (!seeker->Init(mOfflinePartition)) {
        IE_LOG(ERROR, "init partition seeker fail!");
        return PartitionSeekerPtr();
    }
    IE_LOG(INFO, "Create PartitionSeeker for path [%s]", mPath.c_str());
    return seeker;
}

PartitionPatcherPtr PartitionResourceProvider::CreatePartitionPatcher(const IndexPartitionSchemaPtr& newSchema,
                                                                      const file_system::DirectoryPtr& patchDir)
{
    int32_t newId = GetNewSchemaVersionId(newSchema);
    IE_LOG(INFO, "CreatePartitionPatcher for new schema version [%d]", newId);
    PluginManagerPtr pluginManager;
    if (mPluginManagerPair.first != newId) {
        IndexPartitionOptions options = mOptions;
        options.GetOfflineConfig().readerConfig.loadIndex = true;
        pluginManager = IndexPluginLoader::Load(mPluginPath, newSchema->GetIndexSchema(), options);
        CounterMapPtr counterMap(new CounterMap());
        PartitionMeta partitionMeta = mOfflinePartition->GetPartitionMeta();
        PluginResourcePtr resource(
            new IndexPluginResource(newSchema, mOptions, counterMap, partitionMeta, mPluginPath));
        pluginManager->SetPluginResource(resource);
        mPluginManagerPair.first = newId;
        mPluginManagerPair.second = pluginManager;
    } else {
        pluginManager = mPluginManagerPair.second;
    }

    mPatchIndexDir = patchDir;
    assert(mPatchIndexDir);

    PartitionPatcherPtr patcher(new PartitionPatcher);
    if (!patcher->Init(mOfflinePartition, newSchema, mPatchIndexDir, pluginManager)) {
        IE_LOG(ERROR, "init partition patcher fail, path[%s]!", mPath.c_str());
        return PartitionPatcherPtr();
    }
    IE_LOG(INFO, "CreatePartitionPatcher for path [%s], patch dir path [%s]", mPath.c_str(),
           patchDir->DebugString().c_str());
    return patcher;
}

IndexPartitionReaderPtr PartitionResourceProvider::GetReader() const
{
    if (!EnsurePartition(true)) {
        return IndexPartitionReaderPtr();
    }
    if (mOfflinePartition) {
        return mOfflinePartition->GetReader();
    }
    return IndexPartitionReaderPtr();
}

PartitionSegmentIteratorPtr PartitionResourceProvider::CreatePartitionSegmentIterator() const
{
    if (mOfflinePartition) {
        return mOfflinePartition->GetPartitionData()->CreateSegmentIterator();
    }
    return PartitionSegmentIteratorPtr();
}

string PartitionResourceProvider::GetPatchRootDirName(const IndexPartitionSchemaPtr& newSchema)
{
    int32_t newId = GetNewSchemaVersionId(newSchema);
    return PartitionPatchIndexAccessor::GetPatchRootDirName(newId);
}

int32_t PartitionResourceProvider::GetNewSchemaVersionId(const IndexPartitionSchemaPtr& newSchema)
{
    if (newSchema->HasModifyOperations()) {
        return newSchema->GetSchemaVersionId();
    }

    schemavid_t oldId = mSchema->GetSchemaVersionId();
    schemavid_t newId = newSchema->GetSchemaVersionId();
    // if (newId == DEFAULT_SCHEMAID)
    // {
    //     // newSchema verionid : auto inc 1
    //     return oldId + 1;
    // }

    if (oldId >= newId) {
        INDEXLIB_FATAL_ERROR(BadParameter, "versionid in new schema [%u] should be greater than old schema [%u]",
                             newSchema->GetSchemaVersionId(), mSchema->GetSchemaVersionId());
    }
    return newId;
}

void PartitionResourceProvider::StoreVersion(const IndexPartitionSchemaPtr& newSchema, versionid_t versionId)
{
    Version newVersion = GetNewVersion(newSchema, versionId);
    IndexPartitionSchemaPtr schema(newSchema->Clone());
    int32_t newId = GetNewSchemaVersionId(newSchema);
    schema->SetSchemaVersionId(newId);
    string newSchemaPath = newVersion.GetSchemaFileName();
    if (mDirectory->IsExist(newVersion.GetSchemaFileName())) {
        IE_LOG(INFO, "[%s] already exist, auto remove it!", newSchemaPath.c_str());
        mDirectory->RemoveFile(newSchemaPath);
    }
    IE_LOG(INFO, "store new schema [%s] basePath[%s]", newSchemaPath.c_str(), mPath.c_str());

    SchemaAdapter::StoreSchema(mDirectory, schema);
    DumpDeployIndexForPatchSegments(schema);

    IE_LOG(INFO, "commit new version [%d] file", newVersion.GetVersionId());

    newVersion.SetUpdateableSchemaStandards(mOptions.GetUpdateableSchemaStandards());
    VersionCommitter committer(mDirectory, newVersion, mOptions.GetBuildConfig().keepVersionCount);
    committer.Commit();
}

Version PartitionResourceProvider::GetNewVersion(const IndexPartitionSchemaPtr& newSchema, versionid_t versionId)
{
    if (versionId <= mVersion.GetVersionId()) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "invalid target version id [%d], should greater than [%d]", versionId,
                             mVersion.GetVersionId());
    }

    int32_t newId = GetNewSchemaVersionId(newSchema);
    Version newVersion = mVersion;
    newVersion.SetSchemaVersionId(newId);
    newVersion.SetVersionId(versionId);
    return newVersion;
}

void PartitionResourceProvider::DumpDeployIndexForPatchSegments(const IndexPartitionSchemaPtr& newSchema)
{
    Version::Iterator iter = mVersion.CreateIterator();
    while (iter.HasNext()) {
        segmentid_t segId = iter.Next();
        string segPath = mVersion.GetSegmentDirName(segId);
        if (mPatchIndexDir == nullptr || !mPatchIndexDir->IsExist(segPath)) {
            continue;
        }
        auto segDirectory = mPatchIndexDir->GetDirectory(segPath, false);
        if (segDirectory == nullptr) {
            INDEXLIB_FATAL_ERROR(Runtime, "get directory failed, empty dir[%s/%s]",
                                 mPatchIndexDir->DebugString().c_str(), segPath.c_str());
        }
        SegmentFileListWrapper::Dump(segDirectory, "");
    }
}

void PartitionResourceProvider::MountPatchIndex(const std::string& physicalRoot, const std::string& patchDir)
{
    auto fs = mDirectory->GetFileSystem();
    // TODO(xiuchen) optmize, from version or read segment_file_list only
    THROW_IF_FS_ERROR(fs->MountDir(physicalRoot, patchDir, patchDir, FSMT_READ_WRITE, true), "mount dir[%s] failed",
                      patchDir.c_str());

    mPatchIndexDir = mDirectory->GetDirectory(patchDir, true);
    if (mPatchIndexDir == nullptr) {
        INDEXLIB_FATAL_ERROR(Runtime, "get directory failed, empty dir[%s]", mDirectory->DebugString(patchDir).c_str());
    }
    mIsMergeInstance = true;
}
}} // namespace indexlib::partition
