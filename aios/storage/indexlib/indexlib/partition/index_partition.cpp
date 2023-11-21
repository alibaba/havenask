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
#include "indexlib/partition/index_partition.h"

#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "autil/legacy/json.h"
#include "autil/legacy/string_tools.h"
#include "fslib/cache/FSCacheModule.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/common/file_system_factory.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/file_system/FileBlockCacheContainer.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemMetrics.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/index_base/branch_fs.h"
#include "indexlib/index_base/index_meta/index_format_version.h"
#include "indexlib/index_base/index_meta/partition_meta.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/partition/builder_branch_hinter.h"
#include "indexlib/partition/flushed_locator_container.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/partition/index_partition_resource.h"
#include "indexlib/partition/local_index_cleaner.h"
#include "indexlib/plugin/index_plugin_loader.h"
#include "indexlib/plugin/plugin_manager.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/TaskScheduler.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/memory_control/MemoryQuotaController.h"

using namespace std;
using namespace fslib;
using namespace fslib::fs;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::common;
using namespace indexlib::plugin;
using namespace indexlib::index_base;
using namespace indexlib::util;
using namespace indexlib::file_system;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, IndexPartition);

#define IE_LOG_PREFIX mPartitionName.c_str()

IndexPartition::IndexPartition(const IndexPartitionResource& partitionResource)
    : mPartitionName(partitionResource.partitionName)
    , mPartitionMemController(
          new util::PartitionMemoryQuotaController(partitionResource.memoryQuotaController, mPartitionName))
    , mFileBlockCacheContainer(partitionResource.fileBlockCacheContainer)
    , mHinterOption(partitionResource.branchOption)
    , mTaskScheduler(partitionResource.taskScheduler)
    , mMetricProvider(partitionResource.metricProvider)
    , mIndexPluginPath(partitionResource.indexPluginPath)
    , mNeedReload(false)
    , mForceSeekInfo(-1, -1)
{
    assert(partitionResource.taskScheduler);
    assert(partitionResource.memoryQuotaController);

    mFlushedLocatorContainer.reset(new partition::FlushedLocatorContainer(10));
}

IndexPartition::~IndexPartition()
{
    if (!mOpenIndexPrimaryDir.empty()) {
        FileSystem::getCacheModule()->invalidatePath(mOpenIndexPrimaryDir);
    }

    if (!mOpenIndexSecondaryDir.empty()) {
        FileSystem::getCacheModule()->invalidatePath(mOpenIndexSecondaryDir);
    }
}

const file_system::DirectoryPtr& IndexPartition::GetRootDirectory() const { return mRootDirectory; }

file_system::DirectoryPtr IndexPartition::GetFileSystemRootDirectory() const
{
    assert(mFileSystem);
    return mRootDirectory;
}

file_system::IFileSystemPtr IndexPartition::CreateFileSystem(const std::string& rawPrimaryDir,
                                                             const std::string& rawSecondaryDir)
{
    string primaryDir = PathUtil::NormalizePath(rawPrimaryDir);
    string secondaryDir = PathUtil::NormalizePath(rawSecondaryDir);
    auto fileBlockCacheContainer = mFileBlockCacheContainer;
    if (!fileBlockCacheContainer) {
        fileBlockCacheContainer.reset(new FileBlockCacheContainer);
        fileBlockCacheContainer->Init("", nullptr, nullptr, mMetricProvider);
    }
    auto fsOptions = FileSystemFactory::CreateFileSystemOptions(primaryDir, mOptions, mPartitionMemController,
                                                                fileBlockCacheContainer, mPartitionName);
    fsOptions.outputStorage = FSST_MEM;
    if (mOptions.IsOffline() && mOptions.GetBuildConfig().enablePackageFile) {
        fsOptions.outputStorage = FSST_PACKAGE_MEM;
    }
    if (mOptions.IsOffline()) {
        BuilderBranchHinter branchHinter(mHinterOption);
        mBranchFileSystem = BranchFS::CreateWithAutoFork(primaryDir, fsOptions, mMetricProvider, &branchHinter);
        IE_LOG(INFO, "Create Forked Branch FileSystem From Dir[%s], branchName[%s]", primaryDir.c_str(),
               mBranchFileSystem->GetBranchName().c_str());
        return mBranchFileSystem->GetFileSystem();
    } else {
        fsOptions.redirectPhysicalRoot = true;
        auto fs = FileSystemCreator::Create("online", primaryDir, fsOptions, mMetricProvider).GetOrThrow();
        fs->SetDefaultRootPath(primaryDir, secondaryDir);
        string rtIndexPath = PathUtil::JoinPath(primaryDir, RT_INDEX_PARTITION_DIR_NAME);
        string joinIndexPath = PathUtil::JoinPath(primaryDir, JOIN_INDEX_PARTITION_DIR_NAME);
        if (FslibWrapper::IsDir(rtIndexPath).GetOrThrow()) {
            THROW_IF_FS_ERROR(fs->MountDir(primaryDir, RT_INDEX_PARTITION_DIR_NAME, RT_INDEX_PARTITION_DIR_NAME,
                                           FSMT_READ_WRITE, false),
                              "mount dir[%s] failed", RT_INDEX_PARTITION_DIR_NAME.c_str());
        }
        if (FslibWrapper::IsDir(joinIndexPath).GetOrThrow()) {
            THROW_IF_FS_ERROR(fs->MountDir(primaryDir, JOIN_INDEX_PARTITION_DIR_NAME, JOIN_INDEX_PARTITION_DIR_NAME,
                                           FSMT_READ_WRITE, false),
                              "mount dir[%s] failed", RT_INDEX_PARTITION_DIR_NAME.c_str());
        }
        return fs;
    }
}

void IndexPartition::CheckPartitionMeta(const IndexPartitionSchemaPtr& schema, const PartitionMeta& partitionMeta) const
{
    assert(schema);
    const AttributeSchemaPtr& attrSchema = schema->GetAttributeSchema();

    const indexlibv2::config::SortDescriptions& sortDescriptions = partitionMeta.GetSortDescriptions();

    if (!attrSchema && sortDescriptions.size() > 0) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "attribute schema should not be empty.");
    }

    for (size_t i = 0; i < sortDescriptions.size(); ++i) {
        string fieldName = sortDescriptions[i].GetSortFieldName();
        const AttributeConfigPtr& attrConfig = attrSchema->GetAttributeConfig(fieldName);
        if (!attrConfig) {
            INDEXLIB_FATAL_ERROR(InconsistentState,
                                 "No attribute config specified "
                                 "for sort field[%s] in partition meta.",
                                 fieldName.c_str());
        }
        if (attrConfig->GetPackAttributeConfig() != NULL) {
            INDEXLIB_FATAL_ERROR(UnSupported, "sort field[%s] should not be in pack attribute.", fieldName.c_str());
        }
    }
}

IndexPartitionReaderPtr IndexPartition::GetReader(versionid_t versionId) const
{
    INDEXLIB_FATAL_ERROR(UnSupported, "only support GetReader with version in online mode.");
    return IndexPartitionReaderPtr();
}

PartitionMeta IndexPartition::GetPartitionMeta() const
{
    PartitionDataPtr partitionData = mPartitionDataHolder.Get();
    assert(partitionData);
    return partitionData->GetPartitionMeta();
}

FileSystemMetrics IndexPartition::TEST_GetFileSystemMetrics() const
{
    autil::ScopedLock lock(mDataLock);
    return mFileSystem->GetFileSystemMetrics();
}

void IndexPartition::Reset()
{
    mFlushedLocatorContainer->Clear();
    mPartitionDataHolder.Reset();
    mFileSystem.reset();
}

IndexPartition::OpenStatus IndexPartition::Open(const string& primaryDir, const string& secondaryDir,
                                                const IndexPartitionSchemaPtr& schema,
                                                const IndexPartitionOptions& options, versionid_t versionId)
{
    mOpenIndexPrimaryDir = primaryDir;
    mOpenIndexSecondaryDir = secondaryDir;
    mOpenSchema = schema;
    mOpenOptions = options;
    mOptions = options;

    IE_PREFIX_LOG(INFO, "current free memory is %ld MB", mPartitionMemController->GetFreeQuota() / (1024 * 1024));
    mFileSystem = CreateFileSystem(primaryDir, secondaryDir);
    mRootDirectory = Directory::Get(mFileSystem);
    const string& dir = secondaryDir;
    assert(!dir.empty());
    if (!IsEmptyDir(dir, schema)) {
        return OS_OK;
    }
    // prepare empty dir
    if (NotAllowToModifyRootPath()) {
        INDEXLIB_FATAL_ERROR(InitializeFailed,
                             "inc parallel build from empty parent path [%s], "
                             "wait instance[0] branch [0] init parent path!",
                             primaryDir.c_str());
    }
    if (!schema) {
        INDEXLIB_FATAL_ERROR(BadParameter, "Invalid parameter, schema is empty");
    }
    mSchema.reset(schema->Clone());
    SchemaAdapter::RewritePartitionSchema(mSchema, mRootDirectory, mOptions);
    CheckParam(mSchema, mOptions);

    // assert(!mHinterOption.epochId.empty());
    FenceContextPtr fenceContext(mOptions.IsOffline() ? FslibWrapper::CreateFenceContext(dir, mHinterOption.epochId)
                                                      : FenceContext::NoFence());
    CleanRootDir(dir, mSchema, fenceContext.get());
    InitEmptyIndexDir(fenceContext.get());
    return OS_OK;
}

void IndexPartition::InitEmptyIndexDir(FenceContext* fenceContext)
{
    std::string partitionPath = mOpenIndexSecondaryDir.empty() ? mOpenIndexPrimaryDir : mOpenIndexSecondaryDir;
    auto ec = FslibWrapper::MkDirIfNotExist(partitionPath).Code();
    THROW_IF_FS_ERROR(ec, "make partition dir failed. [%s]", partitionPath.c_str());
    IndexFormatVersion binaryVersion(INDEX_FORMAT_VERSION);
    std::string binaryVersionFilePath = PathUtil::JoinPath(partitionPath, INDEX_FORMAT_VERSION_FILE_NAME);
    binaryVersion.Store(binaryVersionFilePath, fenceContext);
    THROW_IF_FS_ERROR(mFileSystem->MountFile(partitionPath, INDEX_FORMAT_VERSION_FILE_NAME,
                                             INDEX_FORMAT_VERSION_FILE_NAME, FSMT_READ_ONLY, -1, false),
                      "mount index format version file failed");

    std::string schemaFileName = Version::GetSchemaFileName(mSchema->GetSchemaVersionId());
    std::string schemaFilePath = PathUtil::JoinPath(partitionPath, schemaFileName);
    SchemaAdapter::StoreSchema(partitionPath, mSchema, fenceContext);
    THROW_IF_FS_ERROR(mFileSystem->MountFile(partitionPath, schemaFileName, schemaFileName, FSMT_READ_ONLY, -1, false),
                      "mount schema file failed");
}

bool IndexPartition::IsEmptyDir(const std::string& root, const IndexPartitionSchemaPtr& schema) const
{
    bool rootExist = false;
    auto ec = FslibWrapper::IsExist(root, rootExist);
    THROW_IF_FS_ERROR(ec, "Check root exist failed. [%s]", root.c_str());
    if (!rootExist) {
        return true;
    }
    Version version;
    VersionLoader::GetVersionS(root, version, INVALID_VERSIONID);
    if (version.GetVersionId() != INVALID_VERSIONID) {
        return false;
    }

    schemaid_t schemaId = schema ? schema->GetSchemaVersionId() : DEFAULT_SCHEMAID;
    std::string schemaFileName = Version::GetSchemaFileName(schemaId);
    std::string schemaFilePath = PathUtil::JoinPath(root, schemaFileName);
    bool schemaExist = FslibWrapper::IsExist(schemaFilePath).GetOrThrow();

    std::string formatVersionFilePath = PathUtil::JoinPath(root, INDEX_FORMAT_VERSION_FILE_NAME);
    bool formatVersionExist = FslibWrapper::IsExist(formatVersionFilePath).GetOrThrow();

    if (schemaExist && formatVersionExist) {
        return false;
    }

    if (!schemaExist && !formatVersionExist) {
        return true;
    }
    return true;
}

void IndexPartition::CleanRootDir(const std::string& root, const IndexPartitionSchemaPtr& schema,
                                  FenceContext* fenceContext)
{
    std::string formatVersionFilePath = PathUtil::JoinPath(root, INDEX_FORMAT_VERSION_FILE_NAME);
    FslibWrapper::DeleteFileE(formatVersionFilePath, DeleteOption::Fence(fenceContext, true));
    schemaid_t schemaId = schema ? schema->GetSchemaVersionId() : DEFAULT_SCHEMAID;
    std::string schemaFileName = Version::GetSchemaFileName(schemaId);
    std::string schemaFilePath = PathUtil::JoinPath(root, schemaFileName);
    FslibWrapper::DeleteFileE(schemaFilePath, DeleteOption::Fence(fenceContext, true));
}

document::Locator IndexPartition::GetLastFlushedLocator() { return mFlushedLocatorContainer->GetLastFlushedLocator(); }

bool IndexPartition::HasFlushingLocator() { return mFlushedLocatorContainer->HasFlushingLocator(); }

void IndexPartition::SetBuildMemoryQuotaControler(const util::QuotaControlPtr& buildMemoryQuotaControler)
{
    mPartitionMemController->SetBuildMemoryQuotaControler(buildMemoryQuotaControler);
}

void IndexPartition::Close()
{
    // assert(mPartitionMemController->GetUsedQuota() == 0);
}

bool IndexPartition::CleanIndexFiles(const string& primaryPath, const string& secondaryPath,
                                     const vector<versionid_t>& keepVersionIds)
{
    try {
        auto isDirRet = FslibWrapper::IsDir(primaryPath);
        if (isDirRet.OK() && isDirRet.GetOrThrow()) {
            LocalIndexCleaner cleaner(primaryPath, secondaryPath);
            return cleaner.Clean(keepVersionIds);
        }
        IE_LOG(INFO, "primaryPath[%s] is not a dir, secondaryPath[%s], ec[%d]", primaryPath.c_str(),
               secondaryPath.c_str(), isDirRet.Code());
        return true;
    } catch (const exception& e) {
        IE_LOG(ERROR, "caught exception: %s", e.what());
    } catch (...) {
        IE_LOG(ERROR, "caught unknown exception");
    }
    return false;
}

int64_t IndexPartition::GetUsedMemory() const { return mPartitionMemController->GetUsedQuota(); }

const file_system::FileBlockCachePtr& IndexPartition::TEST_GetFileBlockCache() const
{
    if (!mFileBlockCacheContainer) {
        static file_system::FileBlockCachePtr EMPTY_FILE_CACHE;
        return EMPTY_FILE_CACHE;
    }
    return mFileBlockCacheContainer->GetAvailableFileCache("");
}

#undef IE_LOG_PREFIX
}} // namespace indexlib::partition
