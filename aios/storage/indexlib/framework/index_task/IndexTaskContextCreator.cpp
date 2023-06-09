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
#include "indexlib/framework/index_task/IndexTaskContextCreator.h"

#include "indexlib/base/PathUtil.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/file_system/EntryTableBuilder.h"
#include "indexlib/file_system/FileBlockCacheContainer.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/load_config/CacheLoadStrategy.h"
#include "indexlib/file_system/relocatable/RelocatableFolder.h"
#include "indexlib/file_system/relocatable/Relocator.h"
#include "indexlib/framework/DiskSegment.h"
#include "indexlib/framework/Fence.h"
#include "indexlib/framework/MetricsManager.h"
#include "indexlib/framework/ResourceMap.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/framework/TabletDataSchemaGroup.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/framework/VersionLoader.h"
#include "indexlib/framework/index_task/IIndexOperationCreator.h"
#include "indexlib/framework/index_task/IndexTaskResourceManager.h"
#include "indexlib/util/PathUtil.h"

namespace indexlibv2::framework {

AUTIL_LOG_SETUP(indexlib.framework, IndexTaskContextCreator);

#define TABLET_LOG(level, format, args...) AUTIL_LOG(level, "[%s] [%p] " format, _tabletName.c_str(), this, ##args)

namespace {

void AddDefaultLoadConfig(indexlib::file_system::LoadConfigList* loadConfigList)
{
    indexlib::file_system::LoadConfig defaultLoadConfig;
    indexlib::file_system::LoadConfig::FilePatternStringVector pattern;
    pattern.push_back(".*");
    defaultLoadConfig.SetFilePatternString(pattern);
    indexlib::file_system::LoadStrategyPtr strategy =
        std::make_shared<indexlib::file_system::CacheLoadStrategy>(/*useDirectIO*/ false, /*cacheDecompressFile*/ true);
    defaultLoadConfig.SetLoadStrategyPtr(strategy);
    defaultLoadConfig.SetName("default_strategy");
    loadConfigList->PushBack(defaultLoadConfig);
}

} // namespace

IndexTaskContextCreator& IndexTaskContextCreator::SetClock(const std::shared_ptr<util::Clock>& clock)
{
    _context._clock = clock;
    return *this;
}

IndexTaskContextCreator& IndexTaskContextCreator::SetTabletName(const std::string& tabletName)
{
    _tabletName = tabletName;
    return *this;
}

IndexTaskContextCreator& IndexTaskContextCreator::SetTaskEpochId(const std::string& taskEpochId)
{
    _context._taskEpochId = taskEpochId;
    return *this;
}

IndexTaskContextCreator& IndexTaskContextCreator::SetExecuteEpochId(const std::string& executeEpochId)
{
    _executeEpochId = executeEpochId;
    return *this;
}

IndexTaskContextCreator& IndexTaskContextCreator::SetTabletFactory(framework::ITabletFactory* factory)
{
    _tabletFactory = factory;
    return *this;
}

IndexTaskContextCreator&
IndexTaskContextCreator::SetMemoryQuotaController(const std::shared_ptr<MemoryQuotaController>& controller)
{
    _memoryQuotaController = controller;
    return *this;
}

IndexTaskContextCreator& IndexTaskContextCreator::AddSourceVersion(const std::string& root, versionid_t srcVersionId)

{
    SrcInfo info {root, srcVersionId};
    _srcInfos.push_back(info);
    return *this;
}

IndexTaskContextCreator& IndexTaskContextCreator::SetDestDirectory(const std::string& destRoot)
{
    _destRoot = destRoot;
    return *this;
}

IndexTaskContextCreator&
IndexTaskContextCreator::SetTabletOptions(const std::shared_ptr<config::TabletOptions>& options)
{
    _context._tabletOptions = options;
    return *this;
}

IndexTaskContextCreator& IndexTaskContextCreator::SetTabletSchema(const std::shared_ptr<config::TabletSchema>& schema)
{
    _context._tabletSchema = schema;
    _tabletSchemaCache[schema->GetSchemaId()] = schema;
    return *this;
}

Status IndexTaskContextCreator::InitResourceManager()
{
    assert(_tabletFactory);
    if (_destRoot.empty() || _context._taskEpochId.empty() || _executeEpochId.empty()) {
        TABLET_LOG(ERROR, "init resource manager failed, dest root[%s], task epoch[%s], execute epoch[%s]",
                   _destRoot.c_str(), _context._taskEpochId.c_str(), _executeEpochId.c_str());
        return Status::InvalidArgs("init resource manager failed");
    }
    std::string taskTempWorkRoot = PathUtil::GetTaskTempWorkRoot(_destRoot, _context._taskEpochId);
    std::string resourceRoot = indexlib::util::PathUtil::JoinPath(taskTempWorkRoot, "resource");
    auto resourceDirectory = indexlib::file_system::Directory::GetPhysicalDirectory(resourceRoot);
    auto resourceManager = std::make_shared<IndexTaskResourceManager>();
    auto creator = _tabletFactory->CreateIndexTaskResourceCreator();
    [[maybe_unused]] auto status = resourceManager->Init(resourceDirectory, _executeEpochId, std::move(creator));
    assert(status.IsOK());
    _context._resourceManager = resourceManager;
    return Status::OK();
}

IndexTaskContextCreator&
IndexTaskContextCreator::SetMetricProvider(const std::shared_ptr<indexlib::util::MetricProvider>& provider)
{
    _context._metricProvider = provider;
    return *this;
}

IndexTaskContextCreator&
IndexTaskContextCreator::SetFinishedOpExecuteEpochIds(const std::map<IndexOperationId, std::string>& epochIds,
                                                      bool useOpFenceDir)
{
    assert(!_destRoot.empty());
    std::map<IndexOperationId, std::shared_ptr<indexlib::file_system::IDirectory>> fenceRoots;
    for (const auto& [id, epochId] : epochIds) {
        auto fenceRoot = GetFenceRoot(epochId);
        auto opFenceRoot = fenceRoot;
        if (useOpFenceDir) {
            opFenceRoot = PathUtil::JoinPath(fenceRoot, std::to_string(id));
        }
        auto fenceRootDir = indexlib::file_system::IDirectory::GetPhysicalDirectory(opFenceRoot);
        fenceRoots[id] = fenceRootDir;
    }
    _context.SetFinishedOpFences(std::move(fenceRoots));
    return *this;
}

IndexTaskContextCreator& IndexTaskContextCreator::SetFileBlockCacheContainer(
    const std::shared_ptr<indexlib::file_system::FileBlockCacheContainer>& fileBlockCacheContainer)
{
    _fileBlockCacheContainer = fileBlockCacheContainer;
    return *this;
}

IndexTaskContextCreator& IndexTaskContextCreator::SetDesignateTask(const std::string& taskType,
                                                                   const std::string& taskName)
{
    _context._designateTask = std::pair(taskType, taskName);
    return *this;
}

IndexTaskContextCreator& IndexTaskContextCreator::AddParameter(const std::string& key, const std::string& value)
{
    _context.AddParameter(key, value);
    return *this;
}

std::string IndexTaskContextCreator::GetFenceRoot(const std::string& epochId) const
{
    assert(!_destRoot.empty());
    assert(!epochId.empty());
    auto fenceName = Fence::GetFenceName(epochId);
    return PathUtil::JoinPath(PathUtil::GetTaskTempWorkRoot(_destRoot, _context._taskEpochId), fenceName);
}

Status IndexTaskContextCreator::PrepareOutput(DirectoryPtr* indexRoot, DirectoryPtr* fenceRoot) const
{
    indexlib::file_system::FileSystemOptions fsOptions;
    fsOptions.isOffline = true;
    fsOptions.outputStorage = indexlib::file_system::FSST_DISK;
    fsOptions.memoryQuotaControllerV2 = _memoryQuotaController;
    if (_context._tabletOptions) {
        fsOptions.loadConfigList = _context._tabletOptions->GetLoadConfigList();
    }
    AddDefaultLoadConfig(&fsOptions.loadConfigList);
    fsOptions.fileBlockCacheContainer = _fileBlockCacheContainer;
    auto [status, fs] = indexlib::file_system::FileSystemCreator::Create(_tabletName, _destRoot, fsOptions,
                                                                         _context._metricProvider, /*isOverride*/ false,
                                                                         indexlib::file_system::FenceContext::NoFence())
                            .StatusWith();
    RETURN_IF_STATUS_ERROR(status, "create file system failed");
    *indexRoot = indexlib::file_system::Directory::Get(fs);

    // eg: __MERGE_TASK__taskEpochId/__FENCE__executeEpochId
    auto fenceRootPath = GetFenceRoot(_executeEpochId);
    std::tie(status, fs) = indexlib::file_system::FileSystemCreator::Create(
                               _tabletName + fenceRootPath, fenceRootPath, fsOptions, _context._metricProvider,
                               /*isOverride*/ false, indexlib::file_system::FenceContext::NoFence())
                               .StatusWith();
    RETURN_IF_STATUS_ERROR(status, "create fence file system failed");
    *fenceRoot = indexlib::file_system::Directory::Get(fs);
    return Status::OK();
}

IndexTaskContextCreator& IndexTaskContextCreator::SetTaskParams(const IndexTaskContext::Parameters& params)
{
    _context._parameters = params;
    return *this;
}

Status IndexTaskContextCreator::PrepareInput(TabletDataPtr* tabletData, std::string& sourceRoot) const
{
    if (!_tabletFactory) {
        TABLET_LOG(ERROR, "create task context without tabletFactory");
        return Status::Corruption("create task context without tabletFactory");
    }

    std::vector<std::shared_ptr<Segment>> segments;
    framework::Version version;
    std::vector<framework::Version> srcVersions;
    auto resourceMap = std::make_shared<framework::ResourceMap>();
    assert(_srcInfos.size() == 1u);
    auto status = LoadSrc(/*srcIndex*/ 0, /*useVirtualSegmentId*/ false, &segments, &srcVersions, &version);
    RETURN_IF_STATUS_ERROR(status, "load src [%s] failed", _srcInfos[0].root.c_str());
    status = version.FromString(srcVersions[0].ToString());
    RETURN_IF_STATUS_ERROR(status, "");
    status = FillSchemaGroup(_srcInfos[0].root, version, resourceMap.get());
    RETURN_IF_STATUS_ERROR(status, "fill schema group failed");
    auto newTabletData = std::make_shared<TabletData>(_tabletName);
    status = newTabletData->Init(version.Clone(), std::move(segments), resourceMap);
    RETURN_IF_STATUS_ERROR(status, "init tablet data failed");
    *tabletData = newTabletData;
    sourceRoot = _srcInfos[0].root;
    return Status::OK();
}

Status IndexTaskContextCreator::LoadSrc(size_t srcIndex, bool useVirtualSegmentId,
                                        std::vector<std::shared_ptr<Segment>>* segments,
                                        std::vector<Version>* srcVersions, Version* globalVersion) const
{
    const auto& srcInfo = _srcInfos[srcIndex];
    std::string srcRoot = srcInfo.root;
    auto dirName = indexlib::util::PathUtil::GetDirName(srcRoot);
    if (!Fence::IsFenceName(dirName)) {
        auto [status, versionRoot] = GetVersionRoot(srcIndex);
        RETURN_IF_STATUS_ERROR(status, "load version [%d] of [%s] failed", srcInfo.versionId, srcInfo.root.c_str());
        srcRoot = versionRoot;
    }

    indexlib::file_system::FileSystemOptions fsOptions;
    fsOptions.outputStorage = indexlib::file_system::FSST_DISK;
    fsOptions.memoryQuotaControllerV2 = _memoryQuotaController;
    if (_context._tabletOptions) {
        fsOptions.loadConfigList = _context._tabletOptions->GetLoadConfigList();
    }
    AddDefaultLoadConfig(&fsOptions.loadConfigList);
    fsOptions.fileBlockCacheContainer = _fileBlockCacheContainer;

    auto [status, srcFs] = indexlib::file_system::FileSystemCreator::CreateForRead(_tabletName + "_" + srcRoot, srcRoot,
                                                                                   fsOptions, _context._metricProvider)
                               .StatusWith();

    RETURN_IF_STATUS_ERROR(status, "create file system [%s] failed", srcRoot.c_str());
    assert(srcFs);
    status = indexlib::file_system::toStatus(srcFs->MountVersion(srcRoot, srcInfo.versionId, /*logicalPath*/ "",
                                                                 indexlib::file_system::FSMT_READ_ONLY, nullptr));
    RETURN_IF_STATUS_ERROR(status, "mount version[%d] from[%s] failed", srcInfo.versionId, srcRoot.c_str());

    auto srcDir = indexlib::file_system::Directory::Get(srcFs);
    framework::Version srcVersion;
    status = framework::VersionLoader::GetVersion(srcDir, srcInfo.versionId, &srcVersion);
    RETURN_IF_STATUS_ERROR(status, "load version[%d] from [%s] failed: %s", srcInfo.versionId, srcRoot.c_str(),
                           status.ToString().c_str());

    for (auto [segId, schemaId] : srcVersion) {
        auto segDirName = srcVersion.GetSegmentDirName(segId);

        framework::SegmentMeta segmentMeta;
        if (useVirtualSegmentId) {
            segmentMeta.segmentId = globalVersion->GetLastSegmentId() + 1;
        } else {
            segmentMeta.segmentId = segId;
        }
        auto srcRootDir = indexlib::file_system::IDirectory::GetPhysicalDirectory(srcInfo.root);
        auto [status, schema] = LoadSchema(srcRootDir, schemaId);
        RETURN_IF_STATUS_ERROR(status, "load schema[%u] from [%s] failed", schemaId, srcRoot.c_str());
        segmentMeta.schema = schema;

        auto [segStatus, segDir] = srcDir->GetIDirectory()->GetDirectory(segDirName).StatusWith();
        RETURN_IF_STATUS_ERROR(segStatus, "get segment dir[%s] failed: %s", segDirName.c_str(),
                               segStatus.ToString().c_str());
        segmentMeta.segmentDir = indexlib::file_system::IDirectory::ToLegacyDirectory(segDir);
        auto readerOption = indexlib::file_system::ReaderOption::PutIntoCache(indexlib::file_system::FSOT_MEM);
        auto segInfoStatus = segmentMeta.segmentInfo->Load(segDir, readerOption);
        RETURN_IF_STATUS_ERROR(segInfoStatus, "load segment info [%s] failed: %s", segDirName.c_str(),
                               segInfoStatus.ToString().c_str());
        framework::BuildResource buildResource;
        buildResource.metricsManager = _context._metricsManager.get();
        auto diskSegment = _tabletFactory->CreateDiskSegment(segmentMeta, buildResource);
        if (!diskSegment) {
            TABLET_LOG(ERROR, "create disk segment [%d] failed", segId);
            return Status::Corruption("create disk segment failed");
        }
        // Estimate Memory?
        status = diskSegment->Open(_memoryQuotaController, framework::DiskSegment::OpenMode::LAZY);
        RETURN_IF_STATUS_ERROR(status, "open segment [%d] failed", segId);
        segments->emplace_back(diskSegment.release());
        globalVersion->AddSegment(segmentMeta.segmentId);
    }
    srcVersions->push_back(std::move(srcVersion));
    return Status::OK();
}

std::pair<Status, std::shared_ptr<config::TabletSchema>>
IndexTaskContextCreator::LoadSchema(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                                    schemaid_t schemaId) const
{
    assert(_tabletFactory);
    if (schemaId == _context._tabletSchema->GetSchemaId()) {
        return {Status::OK(), _context._tabletSchema};
    }

    auto iter = _tabletSchemaCache.find(schemaId);
    if (iter != _tabletSchemaCache.end()) {
        return {Status::OK(), iter->second};
    }
    std::shared_ptr<config::TabletSchema> schema = TabletSchemaLoader::GetSchema(directory, schemaId);
    if (!schema) {
        TABLET_LOG(ERROR, "load schema[%d] failed", schemaId);
        return {Status::InternalError(), nullptr};
    }
    auto status = TabletSchemaLoader::ResolveSchema(_context._tabletOptions, directory->GetOutputPath(), schema.get());
    if (status.IsOK()) {
        _tabletSchemaCache[schemaId] = schema;
    }
    return {status, schema};
}

Status IndexTaskContextCreator::FillSchemaGroup(const std::string& root, const framework::Version& version,
                                                framework::ResourceMap* resourceMap) const
{
    auto dir = indexlib::file_system::IDirectory::GetPhysicalDirectory(root);
    fslib::FileList fileList;
    auto st = TabletSchemaLoader::ListSchema(dir, &fileList);
    RETURN_IF_STATUS_ERROR(st, "listSchema files in dir [%s] failed", root.c_str());
    for (auto& schemaFile : fileList) {
        auto [status, schema] = LoadSchema(dir, TabletSchemaLoader::GetSchemaId(schemaFile));
        RETURN_IF_STATUS_ERROR(status, "load schema file [%s] failed", schemaFile.c_str());
    }

    auto schemaGroup = std::make_shared<TabletDataSchemaGroup>();
    auto [status, schema] = LoadSchema(dir, version.GetSchemaId());
    RETURN_IF_STATUS_ERROR(status, "load schema[%u] failed", version.GetSchemaId());
    schemaGroup->writeSchema = schema;
    std::tie(status, schema) = LoadSchema(dir, version.GetReadSchemaId());
    RETURN_IF_STATUS_ERROR(status, "load schema[%u] failed", version.GetReadSchemaId());
    schemaGroup->onDiskReadSchema = schema;
    std::tie(status, schema) = LoadSchema(dir, version.GetSchemaId());
    RETURN_IF_STATUS_ERROR(status, "load schema[%u] failed", version.GetSchemaId());
    schemaGroup->onDiskWriteSchema = schema;
    schemaGroup->multiVersionSchemas = _tabletSchemaCache;
    status = resourceMap->AddVersionResource(framework::TabletDataSchemaGroup::NAME, schemaGroup);
    RETURN_IF_STATUS_ERROR(status, "add [%s] failed", framework::TabletDataSchemaGroup::NAME);
    return Status::OK();
}

std::pair<Status, std::string> IndexTaskContextCreator::GetVersionRoot(size_t srcIndex) const
{
    const auto& srcInfo = _srcInfos[srcIndex];
    framework::Version version;

    auto versionRootDir = indexlib::file_system::Directory::GetPhysicalDirectory(srcInfo.root);
    auto status = VersionLoader::GetVersion(versionRootDir, srcInfo.versionId, &version);
    RETURN2_IF_STATUS_ERROR(status, "", "load version [%d] from [%s] failed", srcInfo.versionId, srcInfo.root.c_str());
    std::string fenceRoot = indexlib::util::PathUtil::JoinPath(srcInfo.root, version.GetFenceName());
    return {Status::OK(), fenceRoot};
}

Status IndexTaskContextCreator::UpdateMaxMergedId(const std::string& root)
{
    auto physicalDir = indexlib::file_system::Directory::GetPhysicalDirectory(root);
    assert(physicalDir);
    fslib::FileList versionList;
    auto status = VersionLoader::ListVersion(physicalDir, &versionList);
    RETURN_IF_STATUS_ERROR(status, "list version in [%s] failed", root.c_str());
    fslib::FileList segmentList;
    status = VersionLoader::ListSegment(physicalDir, &segmentList);
    RETURN_IF_STATUS_ERROR(status, "list segment in [%s] failed", root.c_str());

    versionid_t versionId = INVALID_VERSIONID;
    segmentid_t segmentId = INVALID_SEGMENTID;

    for (const auto& name : versionList) {
        auto tmpId = VersionLoader::GetVersionId(name);
        if (tmpId & Version::PUBLIC_VERSION_ID_MASK || tmpId & Version::PRIVATE_VERSION_ID_MASK) {
            continue;
        }
        versionId = std::max(versionId, tmpId);
    }
    for (const auto& name : segmentList) {
        auto tmpId = PathUtil::GetSegmentIdByDirName(name);
        if (tmpId & Segment::PUBLIC_SEGMENT_ID_MASK || tmpId & Segment::PRIVATE_SEGMENT_ID_MASK) {
            continue;
        }
        segmentId = std::max(segmentId, tmpId);
    }
    _context._maxMergedVersionId = std::max(_context._maxMergedVersionId, versionId);
    _context._maxMergedSegmentId = std::max(_context._maxMergedSegmentId, segmentId);
    return Status::OK();
}

std::unique_ptr<IndexTaskContext> IndexTaskContextCreator::CreateContext()
{
    if (!_basicInited) {
        _context._metricsManager.reset(new MetricsManager(
            _tabletName, _context._metricProvider ? _context._metricProvider->GetReporter() : nullptr));
        auto status = InitResourceManager();
        if (!status.IsOK()) {
            TABLET_LOG(ERROR, "init resource manager failed: %s", status.ToString().c_str());
            return nullptr;
        }
        _context._taskTempWorkRoot = PathUtil::GetTaskTempWorkRoot(_destRoot, _context._taskEpochId);
        if (_srcInfos.empty()) {
            auto ret = indexlib::file_system::EntryTableBuilder::GetLastVersion(_destRoot);
            if (!ret.OK()) {
                TABLET_LOG(ERROR, "list last version failed, root[%s], ec[%d]", _destRoot.c_str(), ret.ec);
                return nullptr;
            }
            auto versionId = ret.result;
            SrcInfo info;
            info.root = _destRoot;
            info.versionId = versionId;
            _srcInfos.push_back(info);
        }
        if (!_fileBlockCacheContainer) {
            _fileBlockCacheContainer = std::make_shared<indexlib::file_system::FileBlockCacheContainer>();
            _fileBlockCacheContainer->Init(/*configStr*/ "", /*globalMemoryQuotaController*/ nullptr,
                                           /*taskScheduler*/ nullptr,
                                           /*metricProvider*/ nullptr);
        }
        for (const auto& info : _srcInfos) {
            auto status = UpdateMaxMergedId(info.root);
            if (!status.IsOK()) {
                TABLET_LOG(ERROR, "update max merged id in path[%s] failed: %s", info.root.c_str(),
                           status.ToString().c_str());
                return nullptr;
            }
        }
        status = UpdateMaxMergedId(_destRoot);
        if (!status.IsOK()) {
            TABLET_LOG(ERROR, "update max merged id in path[%s] failed: %s", _destRoot.c_str(),
                       status.ToString().c_str());
            return nullptr;
        }
        _basicInited = true;
    }
    auto status = PrepareInput(&(_context._tabletData), _context._sourceRoot);
    if (!status.IsOK()) {
        TABLET_LOG(ERROR, "prepare filesystem failed: %s", status.ToString().c_str());
        return nullptr;
    }

    status = PrepareOutput(&(_context._indexRoot), &(_context._fenceRoot));
    if (!status.IsOK()) {
        TABLET_LOG(ERROR, "prepare filesystem failed: %s", status.ToString().c_str());
        return nullptr;
    }
    _context._tabletSchema = _context._tabletData->GetWriteSchema();

    auto ret = std::make_unique<IndexTaskContext>(_context);
    return ret;
}

} // namespace indexlibv2::framework
