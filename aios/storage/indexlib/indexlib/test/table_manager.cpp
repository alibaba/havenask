#include "indexlib/test/table_manager.h"

#include <future>
#include <thread>

#include "fslib/util/FileUtil.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/document.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileBlockCacheContainer.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/IndexFileList.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/TabletDeployer.h"
#include "indexlib/framework/VersionDeployDescription.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/partition/index_builder.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/index_partition_creator.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/test/directory_creator.h"
#include "indexlib/test/document_parser.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/result.h"
#include "indexlib/test/result_checker.h"
#include "indexlib/test/simple_table_reader.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/TaskScheduler.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::test;
using namespace indexlib::index;
using namespace indexlib::partition;
using namespace indexlib::index_base;
using namespace indexlib::file_system;
using namespace indexlib::document;
using namespace indexlib::common;
using namespace indexlib::util;

using FSEC = indexlib::file_system::ErrorCode;
namespace indexlib { namespace test {
IE_LOG_SETUP(test, TableManager);

TableManager::TableManager(const config::IndexPartitionSchemaPtr& schema, const config::IndexPartitionOptions& options,
                           const string& indexPluginPath, const string& offlineIndexDir, const string& onlineIndexDir)
    : mSchema(schema)
    , mOptions(options)
    , mPluginRoot(indexPluginPath)
    , mOfflineIndexRoot(offlineIndexDir)
    , mOnlineIndexRoot(onlineIndexDir)
    , mOnlineIsReady(false)
{
}

TableManager::~TableManager() {}

bool TableManager::Init(const util::KeyValueMap& psmOptions, const std::string& onlineTabletConfigStr)
{
    IndexPartitionResource offlineResource;
    offlineResource.indexPluginPath = mPluginRoot;
    mOfflinePsm.reset(new PartitionStateMachine(false, offlineResource));
    mPsmOptions = psmOptions;
    mTabletConfigStr = onlineTabletConfigStr;
    IndexPartitionOptions offlineOptions = mOptions;
    offlineOptions.SetIsOnline(false);
    offlineOptions.GetOnlineConfig() = OnlineConfig();
    offlineOptions.GetOnlineConfig().buildConfig = mOptions.GetBuildConfig(true);
    if (!mOfflinePsm->Init(mSchema, offlineOptions, mOfflineIndexRoot, "offline_psm")) {
        IE_LOG(ERROR, "init offline PSM failed");
        return false;
    }
    THROW_IF_FS_ERROR(file_system::FslibWrapper::MkDirIfNotExist(mOnlineIndexRoot).Code(), "path[%s]",
                      mOnlineIndexRoot.c_str());
    IndexPartitionResourcePtr onlineResource(new IndexPartitionResource);
    auto onlinePartition = PrepareOnlinePartition(onlineResource, INVALID_VERSION);
    if (onlinePartition) {
        mOnlinePartition = onlinePartition;
        mOnlineResource = onlineResource;
        mOnlineIsReady = true;
    } else {
        mOnlineIsReady = false;
    }
    auto iter = psmOptions.begin();
    while (iter != psmOptions.end()) {
        mOfflinePsm->SetPsmOption(iter->first, iter->second);
        iter++;
    }
    return true;
}

IndexPartitionPtr TableManager::PrepareOnlinePartition(const IndexPartitionResourcePtr& onlineResource,
                                                       versionid_t versionId)
{
    IndexPartitionPtr onlinePartition;
    onlineResource->indexPluginPath = mPluginRoot;
    onlineResource->memoryQuotaController =
        MemoryQuotaControllerCreator::CreateMemoryQuotaController(DEFAULT_MEMORY_USE_LIMIT * 1024 * 1024);
    onlineResource->taskScheduler.reset(new TaskScheduler);
    mMetricProvider.reset(new util::MetricProvider(nullptr));
    onlineResource->metricProvider = mMetricProvider;
    onlineResource->fileBlockCacheContainer.reset(new file_system::FileBlockCacheContainer());
    onlineResource->fileBlockCacheContainer->Init("", onlineResource->memoryQuotaController,
                                                  onlineResource->taskScheduler, onlineResource->metricProvider);
    onlineResource->partitionName = "test_table";

    IndexPartitionOptions onlineOptions = mOptions;
    onlineOptions.SetIsOnline(true);
    if (!mTabletConfigStr.empty()) {
        FromJsonString(onlineOptions, mTabletConfigStr);
    }

    string schemaRoot = (onlineOptions.GetOnlineConfig().NeedReadRemoteIndex() && !mOfflineIndexRoot.empty())
                            ? mOfflineIndexRoot
                            : mOnlineIndexRoot;
    onlinePartition = IndexPartitionCreator(*onlineResource).CreateByLoadSchema(mOptions, schemaRoot, INVALID_VERSION);
    if (!onlinePartition) {
        IE_LOG(ERROR, "fail to create OnlinePartition from online index path[%s]", mOnlineIndexRoot.c_str());
        return IndexPartitionPtr();
    }

    IndexPartition::OpenStatus os =
        onlinePartition->Open(mOnlineIndexRoot, mOfflineIndexRoot, mSchema, onlineOptions, versionId);

    if (os == IndexPartition::OS_OK) {
        return onlinePartition;
    } else {
        return IndexPartitionPtr();
    }
}

bool TableManager::BuildIncAndRt(const std::string& docString, TableManager::OfflineBuildFlag flag)
{
    future<bool> rtBuildFuture = std::async(std::launch::async, &TableManager::BuildRt, this, docString);
    future<bool> incBuildFuture = std::async(std::launch::async, &TableManager::BuildInc, this, docString, flag);
    return rtBuildFuture.get() && incBuildFuture.get();
}

bool TableManager::BuildFull(const std::string& fullDocString, TableManager::OfflineBuildFlag flag)
{
    return DoOfflineBuild(fullDocString, true, flag);
}

bool TableManager::BuildInc(const std::string& incDocString, TableManager::OfflineBuildFlag flag)
{
    return DoOfflineBuild(incDocString, false, flag);
}

bool TableManager::DoOfflineBuild(const std::string& docString, bool isFullBuild, TableManager::OfflineBuildFlag flag)
{
    ScopedLock lock(mOfflineBuildLock);
    if (!mOfflinePsm) {
        IE_LOG(ERROR, "offline Psm is NULL");
        return false;
    }
    PsmEvent psmEvent = isFullBuild ? BUILD_FULL : BUILD_INC;
    bool ret = mOfflinePsm->Transfer(psmEvent, docString, "", "");
    if (!ret) {
        return false;
    }

    if (flag & OFB_NEED_DEPLOY) {
        ret = DeployVersion(INVALID_VERSION);
        if (!ret) {
            return false;
        }
    }

    if (flag & OFB_NEED_REOPEN) {
        ret = ReOpenVersion(INVALID_VERSION, ReOpenFlag::RO_NORMAL);
    }
    return ret;
}

IndexBuilderPtr TableManager::InitOnlineBuilder(const IndexPartitionPtr& onlinePartition)
{
    uint64_t memUse = mOptions.GetOnlineConfig().buildConfig.buildTotalMemory;
    QuotaControlPtr quotaControl(new QuotaControl(memUse * 1024 * 1024));
    IndexBuilderPtr builder(new IndexBuilder(onlinePartition, quotaControl, mMetricProvider));
    if (!builder->Init()) {
        IE_LOG(ERROR, "Init Online builder failed");
        return IndexBuilderPtr();
    }
    return builder;
}

void TableManager::StopRealtimeBuild()
{
    if (!mOnlineIsReady) {
        IE_LOG(WARN, "online is not ready");
        return;
    }
    ScopedLock lock(mOnlineBuildLock);
    if (mOnlineBuilder) {
        mOnlineBuilder->EndIndex();
        mOnlineBuilder.reset();
    }
}

bool TableManager::BuildRt(const string& docString)
{
    if (!mOnlineIsReady) {
        IE_LOG(ERROR, "buildRt failed: Online is not ready");
        return false;
    }

    ScopedLock lock(mOnlineBuildLock);
    if (!mOnlineBuilder) {
        IndexBuilderPtr builder = InitOnlineBuilder(mOnlinePartition);
        if (!builder) {
            IE_LOG(ERROR, "init online builder failed");
            return false;
        }
        mOnlineBuilder = builder;
    }
    vector<DocumentPtr> docs;
    if (mPsmOptions["documentFormat"] == "binary") {
        docs = mOfflinePsm->DeserializeDocuments(docString);
    } else {
        docs = mOfflinePsm->CreateDocuments(docString);
    }
    size_t failCount = 0;
    for (size_t i = 0; i < docs.size(); ++i) {
        if (!mOnlineBuilder->Build(docs[i])) {
            failCount += 1;
            IE_LOG(ERROR, "fail to build [%zu]th doc", i);
        }
    }
    return failCount == 0;
}

bool TableManager::CheckResult(const vector<string>& queryVec, const vector<string>& expectResultVec) const
{
    if (queryVec.size() != expectResultVec.size()) {
        IE_LOG(ERROR, "query set size [%zu] does not match result set size[%zu]", queryVec.size(),
               expectResultVec.size());
    }
    bool allMatch = true;
    for (size_t i = 0; i < queryVec.size(); ++i) {
        ResultPtr actualResult = Search(queryVec[i], SearchFlag::SF_ONLINE);
        if (!ResultChecker::Check(actualResult, DocumentParser::ParseResult(expectResultVec[i]))) {
            IE_LOG(ERROR, "[%zu]th query [%s] result does not match expect", i, queryVec[i].c_str());
            allMatch = false;
        }
    }
    return allMatch;
}

ResultPtr TableManager::Search(const std::string& query, TableManager::SearchFlag flag) const
{
    ResultPtr result;
    if (flag == SF_OFFLINE) {
        if (!mOfflinePsm) {
            IE_LOG(ERROR, "offline Psm is NULL");
            return result;
        }
        return mOfflinePsm->Search(query, tsc_no_cache);
    }

    if (!mOnlineIsReady) {
        return result;
    }
    auto partitionReader = mOnlinePartition->GetReader();
    if (!partitionReader) {
        IE_LOG(ERROR, "PartitionReader is NULL");
        return result;
    }
    auto tableReader = partitionReader->GetTableReader();
    if (!tableReader) {
        IE_LOG(ERROR, "TableReader is NULL");
        return result;
    }
    SimpleTableReaderPtr simpleTableReader = DYNAMIC_POINTER_CAST(SimpleTableReader, tableReader);

    if (!simpleTableReader) {
        IE_LOG(ERROR, "TableManager only supports TableReader"
                      " derived from SimpleTableReader");
        return result;
    }
    return simpleTableReader->Search(query);
}

bool TableManager::DeployAndLoadVersion(versionid_t versionId)
{
    bool ret = false;
    ret = DeployVersion(versionId);
    if (!ret) {
        return false;
    }
    ret = ReOpenVersion(INVALID_VERSION, ReOpenFlag::RO_NORMAL);
    if (!ret) {
        return false;
    }
    return true;
}

bool TableManager::LegacyDeployVersion(versionid_t versionId)
{
    ScopedLock lock(mDeployLock);
    IE_LOG(INFO, "begin deploy version[%d]", versionId);
    try {
        versionid_t onlineVersionId = INVALID_VERSION;
        Version offlineVersion;
        VersionLoader::GetVersionS(mOfflineIndexRoot, offlineVersion, versionId);

        versionid_t offlineVersionId = offlineVersion.GetVersionId();
        fslib::FileList versionList;
        VersionLoader::ListVersionS(mOnlineIndexRoot, versionList);

        if (!versionList.empty()) {
            onlineVersionId = VersionLoader::GetVersionId(*versionList.rbegin());
        }
        IndexFileList deployIndexMeta;
        DeployIndexWrapper::GetDeployIndexMetaInputParams inputParams;
        inputParams.localPath = mOnlineIndexRoot;
        inputParams.rawPath = mOfflineIndexRoot;
        inputParams.remotePath = inputParams.rawPath;
        inputParams.baseOnlineConfig = &mOptions.GetOnlineConfig();
        inputParams.targetOnlineConfig = &mOptions.GetOnlineConfig();
        inputParams.baseVersionId = onlineVersionId;
        inputParams.targetVersionId = offlineVersionId;
        indexlib::file_system::DeployIndexMetaVec localDeployIndexMetaVec;
        indexlib::file_system::DeployIndexMetaVec remoteDeployIndexMetaVec;
        if (!DeployIndexWrapper::GetDeployIndexMeta(inputParams, localDeployIndexMetaVec, remoteDeployIndexMetaVec)) {
            IE_LOG(ERROR, "get deploy meta failed for remote[%d] and local[%d]", offlineVersionId, onlineVersionId)
            return false;
        }
        for (const auto& deployIndexMeta : localDeployIndexMetaVec) {
            DoDeployVersion(*deployIndexMeta, offlineVersion, mOfflineIndexRoot, mOnlineIndexRoot);
        }

    } catch (const ExceptionBase& e) {
        IE_LOG(ERROR, "deploy version[%d] failed", versionId);
        return false;
    }
    IE_LOG(INFO, "deploy version[%d] success", versionId);
    return true;
}

bool TableManager::DeployVersion(versionid_t versionId)
{
    if (mTabletConfigStr.empty()) {
        return LegacyDeployVersion(versionId);
    }
    ScopedLock lock(mDeployLock);
    IE_LOG(INFO, "begin deploy version[%d]", versionId);

    try {
        versionid_t onlineVersionId = INVALID_VERSION;
        Version offlineVersion;
        VersionLoader::GetVersionS(mOfflineIndexRoot, offlineVersion, versionId);

        versionid_t offlineVersionId = offlineVersion.GetVersionId();
        fslib::FileList versionList;
        VersionLoader::ListVersionS(mOnlineIndexRoot, versionList);

        if (!versionList.empty()) {
            onlineVersionId = VersionLoader::GetVersionId(*versionList.rbegin());
        }

        auto versionFile = file_system::FslibWrapper::JoinPath(mOnlineIndexRoot, GetVersionFileName(offlineVersionId));
        auto baseDoneFile = file_system::FslibWrapper::JoinPath(
            mOnlineIndexRoot,
            indexlibv2::framework::VersionDeployDescription::GetDoneFileNameByVersionId(onlineVersionId));
        auto targetDoneFile = file_system::FslibWrapper::JoinPath(
            mOnlineIndexRoot,
            indexlibv2::framework::VersionDeployDescription::GetDoneFileNameByVersionId(offlineVersionId));

        indexlibv2::framework::VersionDeployDescription baseVersionDpDesc;
        if (!LoadDeployDone(baseDoneFile, baseVersionDpDesc)) {
            IE_LOG(WARN, "base version[%d] done file load failed", onlineVersionId);
        }

        indexlibv2::config::TabletOptions tabletOptions;
        autil::legacy::FromJsonString(tabletOptions, mTabletConfigStr);

        indexlibv2::framework::TabletDeployer::GetDeployIndexMetaInputParams inputParams;
        inputParams.rawPath = mOfflineIndexRoot;
        inputParams.localPath = mOnlineIndexRoot;
        inputParams.remotePath = mOfflineIndexRoot;
        inputParams.baseTabletOptions = &tabletOptions;
        inputParams.targetTabletOptions = &tabletOptions;
        inputParams.baseVersionId = onlineVersionId;
        inputParams.targetVersionId = offlineVersionId;
        inputParams.baseVersionDeployDescription = baseVersionDpDesc;
        indexlibv2::framework::TabletDeployer::GetDeployIndexMetaOutputParams outputParams;
        if (!indexlibv2::framework::TabletDeployer::GetDeployIndexMeta(inputParams, &outputParams)) {
            IE_LOG(ERROR, "get deploy meta failed for remote[%d] and local[%d]", offlineVersionId, onlineVersionId);
            return false;
        }
        auto targetVersionDpDesc = outputParams.versionDeployDescription;
        if (outputParams.localDeployIndexMetaVec.empty()) {
            return MarkDeployDone(targetDoneFile, targetVersionDpDesc);
        } else {
            if (CheckDeployDone(versionFile, targetDoneFile, targetVersionDpDesc)) {
                return true;
            }
            for (const auto& deployIndexMeta : outputParams.localDeployIndexMetaVec) {
                DoDeployVersion(*deployIndexMeta, offlineVersion, mOfflineIndexRoot, mOnlineIndexRoot);
            }
            return MarkDeployDone(targetDoneFile, targetVersionDpDesc);
        }
    } catch (const ExceptionBase& e) {
        IE_LOG(ERROR, "deploy version[%d] failed", versionId);
        return false;
    }
    IE_LOG(INFO, "deploy version[%d] success", versionId);
    return true;
}

void TableManager::DoDeployVersion(const IndexFileList& dpMeta, const Version& targetVersion, const string& srcRoot,
                                   const string& dstRoot)
{
    auto CopyFile = [&srcRoot, &dstRoot](const string& fileName) {
        string fullSrcPath = file_system::FslibWrapper::JoinPath(srcRoot, fileName);
        string fullDstPath = file_system::FslibWrapper::JoinPath(dstRoot, fileName);
        THROW_IF_FS_ERROR(
            file_system::FslibWrapper::DeleteDir(fullDstPath, file_system::DeleteOption::NoFence(true)).Code(), "%s",
            fullDstPath.c_str());
        THROW_IF_FS_ERROR(file_system::FslibWrapper::Copy(fullSrcPath, fullDstPath).Code(), "%s to %s",
                          fullSrcPath.c_str(), fullDstPath.c_str());
    };

    for (const auto& fileMeta : dpMeta.deployFileMetas) {
        if (fileMeta.isDirectory()) {
            string fullSrcPath = file_system::FslibWrapper::JoinPath(srcRoot, fileMeta.filePath);
            string fullDstPath = file_system::FslibWrapper::JoinPath(dstRoot, fileMeta.filePath);
            THROW_IF_FS_ERROR(file_system::FslibWrapper::MkDirIfNotExist(fullDstPath).Code(), "%s",
                              fullDstPath.c_str());
        }
    }
    for (const auto& fileMeta : dpMeta.deployFileMetas) {
        if (!fileMeta.isDirectory()) {
            CopyFile(fileMeta.filePath);
        }
    }
    string versionFileName = targetVersion.GetVersionFileName();
    CopyFile(versionFileName);
}

bool TableManager::ReOpenVersion(versionid_t versionId, TableManager::ReOpenFlag flag)
{
    ScopedLock lock(mOpenLock);
    if (!mOnlineIsReady) {
        IndexPartitionResourcePtr onlineResource(new IndexPartitionResource);
        auto onlinePartition = PrepareOnlinePartition(onlineResource, versionId);
        if (!onlinePartition) {
            IE_LOG(ERROR, "open version[%d] failed", versionId);
            return false;
        }
        mOnlinePartition = onlinePartition;
        mOnlineResource = onlineResource;
        mOnlineIsReady = true;
        return true;
    }

    if (flag == ReOpenFlag::RO_FORCE) {
        ScopedLock lock(mOnlineBuildLock);
        mOnlineIsReady = false;
        // wait for reader to be released
        sleep(3);
        if (mOnlineBuilder) {
            mOnlineBuilder->EndIndex();
            mOnlineBuilder.reset();
        }
        IndexPartition::OpenStatus rs = mOnlinePartition->ReOpen(true, versionId);

        if (rs != IndexPartition::OS_OK) {
            return false;
        }
        mOnlineIsReady = true;
        return true;
    }
    IndexPartition::OpenStatus rs = mOnlinePartition->ReOpen(false, versionId);
    return rs == IndexPartition::OS_OK;
}

bool TableManager::CheckDeployDone(const std::string& versionFile, const std::string& doneFile,
                                   const indexlibv2::framework::VersionDeployDescription& targetDpDescription)
{
    bool exist = false;
    if (!fslib::util::FileUtil::isFile(doneFile, exist) || !exist) {
        IE_LOG(WARN, "doneFile:[%s] not exist", doneFile.c_str());
        return false;
    }
    exist = false;
    if (!versionFile.empty() && (!fslib::util::FileUtil::isFile(versionFile, exist) || !exist)) {
        IE_LOG(WARN, "versionFile:[%s] not exist", versionFile.c_str());
        return false;
    }
    string actualContent;
    if (!fslib::util::FileUtil::readFile(doneFile, actualContent)) {
        IE_LOG(WARN, "failed to read doneFile:[%s] content", doneFile.c_str());
        return false;
    }
    if (!targetDpDescription.CheckDeployDone(actualContent)) {
        IE_LOG(WARN, "DeployDescription diff from last deployment, will trigger new deployment");
        return false;
    }
    IE_LOG(INFO, "doneFile:[%s] already exists, deploy success", doneFile.c_str());
    return true;
}

bool TableManager::MarkDeployDone(const std::string& doneFile,
                                  const indexlibv2::framework::VersionDeployDescription& targetDpDescription)
{
    if (!CleanDeployDone(doneFile)) {
        return false;
    }
    string content = ToJsonString(targetDpDescription);
    bool ret = fslib::util::FileUtil::writeFile(doneFile, content);
    if (!ret) {
        IE_LOG(ERROR, "mark deploy done failed, content: %s, doneFile: %s", content.c_str(), doneFile.c_str());
    }
    return ret;
}

bool TableManager::CleanDeployDone(const string& doneFile)
{
    bool exist = false;
    if (!fslib::util::FileUtil::isFile(doneFile, exist)) {
        IE_LOG(ERROR, "failed to check whether doneFile exist, localPath: %s", doneFile.c_str());
        return false;
    }
    if (!exist) {
        return true;
    }
    bool ret = fslib::util::FileUtil::remove(doneFile);
    if (!ret) {
        IE_LOG(ERROR, "clean duplicate deploy done file failed, localPath: %s", doneFile.c_str());
    } else {
        IE_LOG(INFO, "clean duplicate deploy done file succeeded, localPath: %s", doneFile.c_str());
    }
    return ret;
}

std::string TableManager::GetVersionFileName(versionid_t versionId)
{
    string versionFileName("version");
    versionFileName += ".";
    versionFileName += StringUtil::toString(versionId);
    return versionFileName;
}

bool TableManager::LoadDeployDone(const string& doneFile,
                                  indexlibv2::framework::VersionDeployDescription& versionDpDescription)
{
    bool exist = false;
    if (!fslib::util::FileUtil::isFile(doneFile, exist) || !exist) {
        IE_LOG(WARN, "doneFile:[%s] not exist", doneFile.c_str());
        return false;
    }
    string fileContent;
    if (!fslib::util::FileUtil::readFile(doneFile, fileContent)) {
        IE_LOG(WARN, "failed to read doneFile:[%s] content", doneFile.c_str());
        return false;
    }
    if (!versionDpDescription.Deserialize(fileContent)) {
        IE_LOG(ERROR, "deserialize doneFile[%s] failed, content=[%s]", doneFile.c_str(), fileContent.c_str());
        return false;
    }
    return true;
}
partition::IndexPartitionReaderPtr TableManager::GetReader() const { return mOnlinePartition->GetReader(); }
}} // namespace indexlib::test
