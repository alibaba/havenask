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
#include "suez/table/SuezIndexPartition.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <string>
#include <unistd.h>
#include <unordered_map>
#include <utility>

#include "autil/Log.h"
#include "autil/SharedPtrUtil.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "autil/legacy/legacy_jsonizable_dec.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/workflow/RealtimeBuilderDefine.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/config/OnlineConfig.h"
#include "indexlib/config/load_config_list.h"
#include "indexlib/file_system/LifecycleConfig.h"
#include "indexlib/index/common/Constant.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/index/kv/Common.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/index_partition_creator.h"
#include "indexlib/partition/partition_group_resource.h"
#include "suez/deploy/FileDeployer.h"
#include "suez/sdk/PathDefine.h"
#include "suez/sdk/SourceReaderProvider.h"
#include "suez/sdk/SuezError.h"
#include "suez/sdk/SuezIndexPartitionData.h"
#include "suez/sdk/TableWriter.h"
#include "suez/table/IndexPartitionAdapter.h"
#include "suez/table/TableMeta.h"
#include "suez/table/TablePathDefine.h"
#include "suez/table/TabletAdapter.h"

using namespace std;
using namespace autil;
using namespace build_service::workflow;

using namespace indexlib::config;
using namespace indexlib::partition;
using namespace indexlib::index_base;

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, SuezIndexPartition);

#define SUEZ_PREFIX ToJsonString(_pid, true).c_str()

namespace suez {

static constexpr int64_t WAIT_TIMEOUT = 10 * 1000 * 1000; // 10s

SuezIndexPartition::SuezIndexPartition(const TableResource &tableResource, const CurrentPartitionMetaPtr &partitionMeta)
    : SuezPartition(partitionMeta, tableResource.pid, tableResource.metricsReporter)
    , _configDeployer(make_unique<FileDeployer>(tableResource.deployManager))
    , _indexDeployer(make_unique<IndexDeployer>(tableResource.pid, tableResource.deployManager))
    , _globalIndexResource(tableResource.globalIndexResource)
    , _globalBuilderThreadResource(tableResource.globalBuilderThreadResource)
    , _swiftClientCreator(tableResource.swiftClientCreator)
    , _allowLoadUtilRtRecovered(tableResource.allowLoadUtilRtRecovered) {
    _refCount = std::make_shared<std::atomic<int32_t>>(0);
}

SuezIndexPartition::~SuezIndexPartition() {
    cancelDeploy();
    unload();
}

StatusAndError<DeployStatus> SuezIndexPartition::deployConfig(const TargetPartitionMeta &target,
                                                              const string &localConfigPath) {
    auto dpRes = doDeployConfig(target.getConfigPath(), localConfigPath);
    return setDeployConfigStatus(dpRes, target);
}

DeployStatus SuezIndexPartition::doDeployConfig(const std::string &src, const std::string &dst) {
    return _configDeployer->deploy(src, dst);
}

StatusAndError<DeployStatus> SuezIndexPartition::deploy(const TargetPartitionMeta &target, bool distDeploy) {
    string localConfigPath = TablePathDefine::constructLocalConfigPath(_pid.getTableName(), target.getConfigPath());
    if (localConfigPath.empty()) {
        SUEZ_PREFIX_LOG(ERROR, "getLocalPath return empty, remote data path: %s", target.getConfigPath().c_str());
        return StatusAndError<DeployStatus>(DS_FAILED, DEPLOY_CONFIG_ERROR);
    }
    auto ret = deployConfig(target, localConfigPath);
    if (ret.status != DS_DEPLOYDONE) {
        SUEZ_PREFIX_LOG(ERROR, "deploy config not done, ds: %s", enumToCStr(ret.status));
        return ret;
    }
    auto dpRes = doDeployIndex(target, distDeploy);
    return setDeployStatus(dpRes, target);
}

DeployStatus SuezIndexPartition::doDeployIndex(const TargetPartitionMeta &target, bool distDeploy) {
    TableConfig targetTableConfig;
    if (!loadTableConfig(target, targetTableConfig)) {
        SUEZ_PREFIX_LOG(ERROR, "can not load online config");
        return DS_FAILED;
    }

    indexlibv2::config::TabletOptions baseTabletOptions;
    const auto &currentConfigPath = _partitionMeta->getConfigPath();
    if (!currentConfigPath.empty() && currentConfigPath != target.getConfigPath()) {
        TableConfig baseTableConfig;
        if (!loadTableConfig(*_partitionMeta, baseTableConfig)) {
            SUEZ_PREFIX_LOG(ERROR, "can not load base TableConfig in path[%s]", currentConfigPath.c_str());
            return DS_FAILED;
        }
        baseTabletOptions = baseTableConfig.getTabletOptions();
    } else {
        baseTabletOptions = targetTableConfig.getTabletOptions();
    }
    //  warmUpRemote is done automatically in deploy
    //  warmUpRemote(target, onlineConfig);
    IndexDeployer::IndexPathDetail pathDetail;
    pathDetail.rawIndexRoot = target.getRawIndexRoot();
    pathDetail.indexRoot = target.getIndexRoot();
    pathDetail.localIndexRoot = PathDefine::getLocalIndexRoot();
    pathDetail.checkIndexPath = target.getCheckIndexPath();
    pathDetail.loadConfigPath = target.getConfigPath();

    const auto &targetTabletOptions = targetTableConfig.getTabletOptions();

    auto dpStatus = _indexDeployer->deploy(pathDetail,
                                           distDeploy ? INVALID_VERSION : _partitionMeta->getIncVersion(),
                                           target.getIncVersion(),
                                           baseTabletOptions,
                                           targetTabletOptions);

    if (!cleanUnreferencedDeployIndexFiles(pathDetail, targetTabletOptions, target.getIncVersion())) {
        SUEZ_PREFIX_LOG(ERROR, "clean deploy index files failed for version[%d]", target.getIncVersion());
    }
    return dpStatus;
}

bool SuezIndexPartition::cleanUnreferencedDeployIndexFiles(const IndexDeployer::IndexPathDetail &pathDetail,
                                                           const indexlibv2::config::TabletOptions &targetTabletOptions,
                                                           IncVersion targetVersion) {
    if (!targetTabletOptions.GetOnlineConfig().EnableLocalDeployManifestChecking()) {
        return true;
    }
    auto indexlibAdapter = getIndexlibAdapter();
    if (indexlibAdapter) {
        // in version rollback scenerio, target.incVersion may be smaller than loadedVersion
        auto whiteListBaseVersion = std::min(_partitionMeta->getIncVersion(), targetVersion);
        if (whiteListBaseVersion == INVALID_VERSION) {
            SUEZ_PREFIX_LOG(INFO,
                            "skip clean index files, currentVersion[%d], targetVersion[%d]",
                            _partitionMeta->getIncVersion(),
                            targetVersion);
            return true;
        }
        // clean previously  generated version.done files, in case IncVersion is rollbacked
        auto removePredicate = [&whiteListBaseVersion](IncVersion versionId) {
            return versionId < whiteListBaseVersion;
        };
        if (!_indexDeployer->cleanDoneFiles(pathDetail.localIndexRoot, removePredicate)) {
            SUEZ_PREFIX_LOG(WARN, "clean donefiles before version[%d] failed", static_cast<int>(whiteListBaseVersion));
            return false;
        }
        // TODO: support keep latest N version.done files
        set<string> dpFileWhiteList;
        if (!_indexDeployer->getNeedKeepDeployFiles(
                pathDetail.localIndexRoot, whiteListBaseVersion, &dpFileWhiteList)) {
            SUEZ_PREFIX_LOG(WARN, "cannot get deploy file whiteList");
            return false;
        }
        return indexlibAdapter->cleanUnreferencedIndexFiles(dpFileWhiteList);
    }
    return true;
}

void SuezIndexPartition::cancelDeploy() {
    _configDeployer->cancel();
    _indexDeployer->cancel();
}

void SuezIndexPartition::cancelLoad() {
    // now we can only cancel load rt process T_T
    stopRt();
}

StatusAndError<TableStatus> SuezIndexPartition::load(const TargetPartitionMeta &target, bool force) {
    if (target.getIncVersion() != INVALID_VERSION) {
        assert(DS_DEPLOYDONE == _partitionMeta->getDeployStatus(target.getIncVersion()));
    }

    if (target.getIncVersion() != INVALID_VERSION && target.getIncVersion() == _partitionMeta->getIncVersion()) {
        SUEZ_PREFIX_LOG(WARN, "load twice for the version %d", target.getIncVersion());
        // maybe return immediately
    }

    SUEZ_PREFIX_LOG(INFO, "load version [%d]", target.getIncVersion());

    ScopedTime2 t;
    TableStatus ts = TS_ERROR_UNKNOWN;
    PartitionProperties properties(_pid);
    if (!initPartitionProperties(target, properties)) {
        SUEZ_PREFIX_LOG(ERROR, "init partition properties failed");
        ts = TS_ERROR_CONFIG;
    } else {
        auto indexlibAdapter = getIndexlibAdapter();
        if (indexlibAdapter.get() == nullptr) {
            ts = loadFull(target, properties);
        } else {
            if (force) {
                doWorkBeforeForceLoad();
            }
            ts = loadInc(properties, target.getBranchId(), force);
        }
    }

    if (ts == TS_LOADED) {
        auto indexlibAdapter = getIndexlibAdapter();
        assert(indexlibAdapter.get() != nullptr);
        if (!doWorkAfterLoaded(indexlibAdapter.get(), properties)) {
            // TODO: reset index partition and partition meta when loadFull
            ts = TS_ERROR_UNKNOWN;
        }
        if (ts == TS_LOADED) {
            updateIncVersion(indexlibAdapter->getLoadedVersion(), indexlibAdapter->getSchema());
        }
    }

    auto ret = constructLoadStatus(ts, force);
    SUEZ_PREFIX_LOG(INFO,
                    "load version [%d] done, actual version [%d] "
                    "tableStatus [%s], loadTime [%f] sec",
                    target.getIncVersion(),
                    _partitionMeta->getIncVersion(),
                    enumToCStr(ts),
                    t.done_sec());
    return ret;
}

void SuezIndexPartition::updateIncVersion(const std::pair<TableVersion, SchemaVersion> &loadedVersion,
                                          const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema) {
    uint32_t lastSchemaVersion = _partitionMeta->getSchemaVersion();
    _partitionMeta->setIncVersion(loadedVersion.first.getVersionId(), loadedVersion.second);
    _partitionMeta->setEffectiveFieldInfo(constructEffectiveFieldInfo(schema));
    bool needUpdateSchemaContent =
        schema != nullptr && (lastSchemaVersion != loadedVersion.second || _partitionMeta->getSchemaContent().empty());
    if (needUpdateSchemaContent) {
        std::string content;
        [[maybe_unused]] bool ret = schema->Serialize(true, &content);
        assert(ret);
        _partitionMeta->setSchemaContent(std::move(content));
    }
}

EffectiveFieldInfo
SuezIndexPartition::constructEffectiveFieldInfo(const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema) {
    EffectiveFieldInfo fieldInfo;
    if (!schema) {
        return fieldInfo;
    }
    const auto &fieldConfigs = schema->GetIndexFieldConfigs(indexlib::GENERALIZED_VALUE_INDEX_TYPE_STR);
    for (const auto &fieldConfig : fieldConfigs) {
        fieldInfo.attributes.emplace_back(fieldConfig->GetFieldName());
    }

    const auto &invertedConfigs = schema->GetIndexConfigs(indexlib::index::INVERTED_INDEX_TYPE_STR);
    for (const auto &invertedConfig : invertedConfigs) {
        fieldInfo.indexes.emplace_back(invertedConfig->GetIndexName());
    }

    const auto &kvConfigs = schema->GetIndexConfigs(indexlib::index::KV_INDEX_TYPE_STR);
    for (const auto &kvConfig : kvConfigs) {
        fieldInfo.indexes.emplace_back(kvConfig->GetIndexName());
    }

    const auto &kkvConfigs = schema->GetIndexConfigs(indexlib::index::KKV_INDEX_TYPE_STR);
    for (const auto &kkvConfig : kkvConfigs) {
        fieldInfo.indexes.emplace_back(kkvConfig->GetIndexName());
    }
    fieldInfo.sort(); //排序是为了方便后面查找fieldInfo里面的元素
    return fieldInfo;
}

StatusAndError<TableStatus> SuezIndexPartition::constructLoadStatus(TableStatus ts, bool force) {
    StatusAndError<TableStatus> ret(ts, ERROR_NONE);
    if (!force) {
        return ret;
    }
    auto indexlibAdapter = getIndexlibAdapter();
    switch (ts) {
    case TS_ERROR_LACK_MEM:
        if (indexlibAdapter) {
            ret.setError(LOAD_FORCE_REOPEN_LACKMEM_ERROR);
        } else {
            ret.setError(LOAD_LACKMEM_ERROR);
        }
        break;
    case TS_ERROR_CONFIG:
        ret.setError(CONFIG_LOAD_ERROR);
        break;
    case TS_ERROR_UNKNOWN:
        if (indexlibAdapter) {
            ret.setError(LOAD_FORCE_REOPEN_UNKNOWN_ERROR);
        } else {
            ret.setError(LOAD_UNKNOWN_ERROR);
        }
        break;
    case TS_FORCE_RELOAD:
        ret.setError(FORCE_RELOAD_ERROR);
        break;
    default:
        ret.setError(ERROR_NONE);
    }
    return ret;
}

TableStatus SuezIndexPartition::loadFull(const TargetPartitionMeta &target, const PartitionProperties &properties) {
    SUEZ_PREFIX_LOG(INFO, "load full with config[%s]", ToJsonString(properties.indexOptions, true).c_str());

    if (properties.directWrite) {
        SUEZ_PREFIX_LOG(INFO, "table writer starting");
        autil::ScopedLock lock(_writerMutex);
        if (!_tableWriter) {
            auto tableWriter = createTableWriter(properties);
            if (!tableWriter) {
                SUEZ_PREFIX_LOG(ERROR, "table writer start failed");
                return TS_ERROR_UNKNOWN;
            }
            // TODO: maybe update writer version
            _tableWriter = std::move(tableWriter);
        } else {
            SUEZ_PREFIX_LOG(INFO, "reuse table writer: %p", _tableWriter.get());
        }
        SUEZ_PREFIX_LOG(INFO, "table writer started");
    }

    auto indexPartition = createIndexlibAdapter(properties, target.getBranchId());
    if (!indexPartition) {
        SUEZ_PREFIX_LOG(ERROR, "create index partition failed");
        return TS_ERROR_UNKNOWN;
    }

    TableVersion version(properties.version);
    version.setBranchId(target.getBranchId());
    auto ts = indexPartition->open(properties, version);
    if (ts != TS_LOADED) {
        return ts;
    }

    // rt
    if ((properties.hasRt || properties.directWrite) && !loadRt(indexPartition.get(), properties)) {
        return TS_ERROR_UNKNOWN;
    }

    setIndexlibAdapter(std::move(indexPartition));

    _partitionMeta->setFullIndexLoaded(true);
    _partitionMeta->setLoadedConfigPath(target.getConfigPath());
    _partitionMeta->setLoadedIndexRoot(target.getIndexRoot());
    _partitionMeta->setBranchId(target.getBranchId());
    _partitionMeta->setRollbackTimestamp(target.getRollbackTimestamp());

    SUEZ_PREFIX_LOG(INFO, "load full end");
    return TS_LOADED;
}

TableStatus SuezIndexPartition::loadInc(const PartitionProperties &properties, uint64_t branchId, bool force) {
    autil::ScopedTime2 timer;
    TableVersion version(properties.version);
    version.setBranchId(branchId);
    if (_partitionMeta->getBranchId() != branchId) {
        SUEZ_PREFIX_LOG(ERROR,
                        "load inc version failed,  because branch id changeed from [%lu] to [%lu], need reload",
                        _partitionMeta->getBranchId(),
                        branchId);

        return TS_FORCE_RELOAD;
    }
    // suez 决策依赖瞬时状态，可能存在对于一个target串行执行两次load的情况
    if (!force && version.getVersionId() == _partitionMeta->getIncVersion()) {
        SUEZ_PREFIX_LOG(
            INFO, "target version [%d] with same branch already loaded, do not load inc", version.getVersionId());
        return TS_LOADED;
    }

    SUEZ_PREFIX_LOG(INFO, "load version [%d] begin force[%d]", version.getVersionId(), int(force));
    bool needReloadRt = false;
    if (hasRt() && force) {
        stopRt();
        needReloadRt = true;
    }

    auto indexlibAdapter = getIndexlibAdapter();
    auto ts = indexlibAdapter->reopen(force, version);
    if (TS_LOADED != ts) {
        SUEZ_PREFIX_LOG(
            INFO, "load version [%d] end [%d] with time [%.3f]s", version.getVersionId(), ts, timer.done_sec());
        return ts;
    }
    if (needReloadRt && !loadRt(indexlibAdapter.get(), properties)) {
        SUEZ_PREFIX_LOG(INFO, "load version [%d] end with time [%.3f]s", version.getVersionId(), timer.done_sec());
        return TS_ERROR_UNKNOWN;
    }

    SUEZ_PREFIX_LOG(INFO, "load version [%d] end with time [%.3f]s", version.getVersionId(), timer.done_sec());
    return TS_LOADED;
}

bool SuezIndexPartition::initPartitionProperties(const TargetPartitionMeta &target,
                                                 PartitionProperties &properties) const {
    TableConfig tableConfig;
    if (!loadTableConfig(target, tableConfig)) {
        SUEZ_PREFIX_LOG(ERROR, "load table config failed");
        return false;
    }
    return properties.init(target, tableConfig);
}

bool SuezIndexPartition::loadTableConfig(const TargetPartitionMeta &target, TableConfig &tableConfig) const {
    auto localConfigPath = TablePathDefine::constructLocalConfigPath(_pid.getTableName(), target.getConfigPath());
    auto indexlibAdapter = getIndexlibAdapter();
    bool isIndexPartition = (dynamic_cast<IndexPartitionAdapter *>(indexlibAdapter.get()) != nullptr);
    return tableConfig.loadFromFile(
        localConfigPath, PathDefine::getTableConfigFileName(_pid.getTableName()), isIndexPartition);
}

std::unique_ptr<IIndexlibAdapter> SuezIndexPartition::createIndexlibAdapter(const PartitionProperties &properties,
                                                                            uint64_t branchId) const {
    string partitionName = TablePathDefine::getIndexPartitionPrefix(_pid);

    IndexPartitionPtr indexPartition =
        IndexPartitionCreator(*_globalIndexResource, partitionName)
            .SetMetricsReporter(_metricsReporter) // table reporter
            .SetIndexPluginPath(PathDefine::join(properties.localConfigPath, "plugins"))
            .SetSrcSignature(properties.realtimeInfo.getSrcSignature())
            .CreateByLoadSchema(properties.indexOptions, properties.schemaRoot, properties.version);
    if (indexPartition.get() == nullptr) {
        return nullptr;
    }
    return std::make_unique<IndexPartitionAdapter>(
        properties.primaryIndexRoot, properties.secondaryIndexRoot, indexPartition);
}

TableBuilder *SuezIndexPartition::createTableBuilder(IIndexlibAdapter *indexlibAdapter,
                                                     const PartitionProperties &properties) const {
    build_service::proto::PartitionId pid;
    if (!getBsPartitionId(properties, pid)) {
        return nullptr;
    }
    RealtimeBuilderResource rtResource(_metricsReporter,
                                       _globalIndexResource->GetTaskScheduler(),
                                       _swiftClientCreator,
                                       _globalBuilderThreadResource,
                                       properties.realtimeInfo);
    return indexlibAdapter->createBuilder(pid, rtResource, properties);
}

std::shared_ptr<TableWriter> SuezIndexPartition::createTableWriter(const PartitionProperties &properties) const {
    SUEZ_PREFIX_LOG(ERROR, "only support table writer in tablet mode");
    return nullptr;
}

bool SuezIndexPartition::loadRt(IIndexlibAdapter *indexlibAdapter, const PartitionProperties &properties) {
    autil::ScopedTime2 timer;
    SUEZ_PREFIX_LOG(INFO, "load rt begin");

    std::unique_ptr<TableBuilder> tableBuilder(createTableBuilder(indexlibAdapter, properties));

    if (!tableBuilder || !tableBuilder->start()) {
        SUEZ_PREFIX_LOG(ERROR, "load rt failed");
        _partitionMeta->setError(LOAD_RT_ERROR);
        return false;
    }

    {
        ScopedLock scopedLock(_builderMutex);
        _tableBuilder = std::move(tableBuilder);
        _partitionMeta->setRtStatus(TRS_BUILDING);
        _realtimeInfo = properties.realtimeInfo;
    }

    // rtBuilder will auto recover if too long wait
    while (_allowLoadUtilRtRecovered) {
        {
            if (_shutdownFlag) {
                SUEZ_PREFIX_LOG(INFO, "stop wait rt loaded for shutdown");
                return true;
            }
            ScopedLock scopedLock(_builderMutex);
            // make sure not reset _rtBuilder in multi_thread
            if (!_tableBuilder) {
                SUEZ_PREFIX_LOG(INFO, "rt builder is deleted when loading.");
                _partitionMeta->setError(LOAD_RT_ERROR);
                return false;
            }
            if (_tableBuilder->isRecovered()) {
                break;
            }
        }
        sleep(1);
    }
    SUEZ_PREFIX_LOG(INFO, "load rt with time [%.3f]", timer.done_sec());

    return true;
}

void SuezIndexPartition::doWorkBeforeForceLoad() {
    {
        autil::ScopedLock lock(_writerMutex);
        if (_tableWriter) {
            _tableWriter->updateIndex(nullptr);
        }
    }
    closeLogReplicator();
}

bool SuezIndexPartition::doWorkAfterLoaded(IIndexlibAdapter *indexlibAdapter, const PartitionProperties &properties) {
    {
        autil::ScopedLock lock(_writerMutex);
        if (_tableWriter) {
            TabletAdapter *tabletAdapter = dynamic_cast<TabletAdapter *>(indexlibAdapter);
            _tableWriter->updateIndex(tabletAdapter->getTablet());
        }
    }
    if (properties.drcConfig.isEnabled()) {
        return initLogReplicator(indexlibAdapter, properties);
    }
    return true;
}

void SuezIndexPartition::stopTableWriter() {
    autil::ScopedLock lock(_writerMutex);
    if (_tableWriter) {
        _tableWriter->stop();
        _tableWriter.reset();
    }
}

bool SuezIndexPartition::initLogReplicator(IIndexlibAdapter *indexlibAdapter, const PartitionProperties &properties) {
    return true;
}

void SuezIndexPartition::closeLogReplicator() {}

void SuezIndexPartition::unload() {
    if (_partitionMeta == nullptr) {
        return;
    }
    _partitionMeta->setFullIndexLoaded(false);
    autil::ScopedTime2 timer;
    SUEZ_PREFIX_LOG(INFO, "unload begin");
    {
        auto indexlibAdapter = getIndexlibAdapter();
        if (indexlibAdapter) {
            indexlibAdapter->reportAccessCounter(_pid.getTableName());
        }
    }
    stopWrite();
    if (unlikely(isInUse())) {
        SUEZ_PREFIX_LOG(ERROR, "use count of index partition is greater than 1, is:[%u]", getUseCount());
    }

    {
        autil::ScopedLock lock(_mutex);
        if (!autil::SharedPtrUtil::waitUseCount(_indexlibAdapter, 1, WAIT_TIMEOUT)) {
            SUEZ_PREFIX_LOG(ERROR, "_indexlibAdapter still in use");
        }
        _indexlibAdapter.reset();
    }
    _partitionMeta->setIncVersion(getUnloadedIncVersion(), 0);
    _partitionMeta->clearEffectiveFieldInfo();
    _partitionMeta->setLoadedIndexRoot("");
    _partitionMeta->setLoadedConfigPath("");
    SUEZ_PREFIX_LOG(INFO, "unload end with time [%.3f] s", timer.done_sec());
}

std::shared_ptr<TableWriter> SuezIndexPartition::getTableWriter() const {
    autil::ScopedLock lock(_writerMutex);
    return _tableWriter;
}

void SuezIndexPartition::setTableWriter(const std::shared_ptr<TableWriter> &writer) {
    autil::ScopedLock lock(_writerMutex);
    SUEZ_PREFIX_LOG(INFO, "set table writer from %p to %p", _tableWriter.get(), writer.get());
    _tableWriter = writer;
}

void SuezIndexPartition::stopWrite(StopWriteOption option) {
    if (option & SWO_WRITE) {
        stopTableWriter();
    }
    if (option & SWO_DRC) {
        closeLogReplicator();
    }
    if (option & SWO_BUILD) {
        stopRt();
    } else {
        // disable commit
        ScopedLock scopedLock(_builderMutex);
        if (_tableBuilder) {
            _tableBuilder->setAllowCommit(false);
        }
    }
}

void SuezIndexPartition::stopRt() {
    SUEZ_PREFIX_LOG(INFO, "stop rt.");
    ScopedLock scopedLock(_builderMutex);
    if (_tableBuilder) {
        _tableBuilder->setAllowCommit(false);
        _tableBuilder->stop();
        _tableBuilder.reset();
    }
    _partitionMeta->setRtStatus(TRS_BUILDING);
    _partitionMeta->setForceOnline(false);
    _partitionMeta->setTimestampToSkip(-1);
    SUEZ_PREFIX_LOG(INFO, "finish stop rt.");
}

void SuezIndexPartition::suspendRt() {
    if (_partitionMeta->getRtStatus() == TRS_SUSPENDED) {
        return;
    }
    ScopedLock scopedLock(_builderMutex);
    if (_tableBuilder) {
        SUEZ_PREFIX_LOG(INFO, "suspend realtime");
        _tableBuilder->suspend();
        _partitionMeta->setRtStatus(TRS_SUSPENDED);
        _partitionMeta->resetError(UPDATE_RT_ERROR);
    } else {
        _partitionMeta->setError(UPDATE_RT_ERROR);
    }
}

void SuezIndexPartition::resumeRt() {
    if (_partitionMeta->getRtStatus() == TRS_BUILDING) {
        return;
    }
    ScopedLock scopedLock(_builderMutex);
    if (_tableBuilder) {
        SUEZ_PREFIX_LOG(INFO, "resume realtime");
        _tableBuilder->resume();
        _partitionMeta->setRtStatus(TRS_BUILDING);
        _partitionMeta->resetError(UPDATE_RT_ERROR);
    } else {
        _partitionMeta->setError(UPDATE_RT_ERROR);
    }
}

bool SuezIndexPartition::hasRt() const {
    ScopedLock scopedLock(_builderMutex);
    return _tableBuilder.get() != nullptr;
}

bool SuezIndexPartition::isRecovered() const {
    ScopedLock scopedLock(_builderMutex);
    if (_tableBuilder) {
        return _tableBuilder->isRecovered();
    }
    return true;
}

bool SuezIndexPartition::initPartitionSourceReaderConfig(PartitionSourceReaderConfig &config) const {
    PartitionProperties properties(_pid);
    config.pid = _pid;
    {
        ScopedLock scopedLock(_builderMutex);
        config.realtimeInfo = _realtimeInfo;
    }
    return true;
}

void SuezIndexPartition::forceOnline() {
    if (_partitionMeta->getForceOnline()) {
        return;
    }
    ScopedLock scopedLock(_builderMutex);
    if (_tableBuilder) {
        SUEZ_PREFIX_LOG(INFO, "force online");
        _tableBuilder->forceRecover();
        _partitionMeta->setForceOnline(true);
        _partitionMeta->resetError(UPDATE_RT_ERROR);
    } else {
        _partitionMeta->setError(UPDATE_RT_ERROR);
    }
}

void SuezIndexPartition::setTimestampToSkip(int64_t timestamp) {
    if (_partitionMeta->getTimestampToSkip() == timestamp) {
        return;
    }
    ScopedLock scopedLock(_builderMutex);
    if (_tableBuilder) {
        SUEZ_PREFIX_LOG(INFO, "set timestampToSkip to %ld", timestamp);
        _tableBuilder->skip(timestamp);
        _partitionMeta->setTimestampToSkip(timestamp);
        _partitionMeta->resetError(UPDATE_RT_ERROR);
    } else {
        _partitionMeta->setError(UPDATE_RT_ERROR);
    }
}

void SuezIndexPartition::cleanIndexFiles(const std::set<IncVersion> &inUseIncVersions) {
    auto indexlibAdapter = getIndexlibAdapter();
    if (!indexlibAdapter) {
        return;
    }

    if (!_indexDeployer->cleanDoneFiles(PathDefine::getLocalIndexRoot(), inUseIncVersions)) {
        return;
    }

    indexlibAdapter->cleanIndexFiles(inUseIncVersions);
}

bool SuezIndexPartition::isInUse() const { return getUseCount() > 0; }

int32_t SuezIndexPartition::getUseCount() const { return _refCount->load(std::memory_order_relaxed); }

SuezPartitionDataPtr SuezIndexPartition::getPartitionData() const {
    auto indexlibAdapter = getIndexlibAdapter();
    if (!indexlibAdapter) {
        return SuezPartitionDataPtr();
    }
    auto partitionData = indexlibAdapter->createSuezPartitionData(
        getPartitionId(), getTotalPartitionCount(), _tableBuilder.get() != nullptr);
    auto refCount = _refCount;
    partitionData->setOnRelease([refCount]() mutable { refCount->fetch_sub(1, std::memory_order_relaxed); });
    _refCount->fetch_add(1, std::memory_order_relaxed);
    return partitionData;
}

bool SuezIndexPartition::getBsPartitionId(const PartitionProperties &properties,
                                          build_service::proto::PartitionId &pid) const {
    pid.mutable_range()->set_from(_pid.from);
    pid.mutable_range()->set_to(_pid.to);
    pid.mutable_buildid()->set_generationid(_pid.getFullVersion());
    *pid.add_clusternames() = _pid.getTableName();
    return true;
}

std::shared_ptr<IIndexlibAdapter> SuezIndexPartition::getIndexlibAdapter() const {
    autil::ScopedLock lock(_mutex);
    return _indexlibAdapter;
}

void SuezIndexPartition::setIndexlibAdapter(std::shared_ptr<IIndexlibAdapter> indexlibAdapter) {
    autil::ScopedLock lock(_mutex);
    if (!autil::SharedPtrUtil::waitUseCount(_indexlibAdapter, 1, WAIT_TIMEOUT)) {
        // maybe wait until ref count = 1 ?
        SUEZ_PREFIX_LOG(ERROR, "_indexlibAdapter still in use");
    }
    if (_indexlibAdapter.get() != nullptr) {
        SUEZ_PREFIX_LOG(ERROR, "load full for twice");
    }
    _indexlibAdapter = std::move(indexlibAdapter);
}

} // namespace suez
