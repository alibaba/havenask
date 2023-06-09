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
#include "suez/table/SuezTabletPartition.h"

#include "autil/EnvUtil.h"
#include "autil/Log.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/merge/RemoteTabletMergeController.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/DataDescriptions.h"
#include "build_service/workflow/RealtimeBuilderDefine.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/base/MemoryQuotaController.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/framework/TabletCreator.h"
#include "indexlib/framework/TabletId.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/table/index_task/LocalTabletMergeController.h"
#include "indexlib/util/PathUtil.h"
#include "suez/drc/LogReplicator.h"
#include "suez/sdk/IpUtil.h"
#include "suez/sdk/PathDefine.h"
#include "suez/sdk/TableWriter.h"
#include "suez/table/DummyTableBuilder.h"
#include "suez/table/TablePathDefine.h"
#include "suez/table/TabletAdapter.h"
#include "suez/table/wal/WALStrategy.h"

using namespace std;
using namespace autil;

#define SUEZ_PREFIX ToJsonString(_pid, true).c_str()

namespace suez {

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, SuezTabletPartition);

SuezTabletPartition::SuezTabletPartition(const TableResource &tableResource,
                                         const CurrentPartitionMetaPtr &partitionMeta)
    : SuezIndexPartition(tableResource, partitionMeta)
    , _executor(tableResource.executor)
    , _dumpExecutor(tableResource.dumpExecutor)
    , _taskScheduler(tableResource.taskScheduler) {
    // Tablet mode support load empty version, target starts from INVALID_VERSION
    // make sure target_version > current_version to invoke a load
    _partitionMeta->setIncVersion(getUnloadedIncVersion());
}

SuezTabletPartition::~SuezTabletPartition() {}

bool SuezTabletPartition::needCommit() const {
    autil::ScopedLock lock(_builderMutex);
    if (_tableBuilder) {
        return _tableBuilder->needCommit();
    }
    return false;
}

pair<bool, TableVersion> SuezTabletPartition::commit() {
    autil::ScopedLock lock(_builderMutex);
    if (_tableBuilder) {
        auto commitResultPair = _tableBuilder->commit();
        if (commitResultPair.first) {
            auto &version = commitResultPair.second;
            version.setBranchId(_partitionMeta->getBranchId());
        }
        return commitResultPair;
    }
    return {false, TableVersion()};
}

DeployStatus SuezTabletPartition::doDeployIndex(const TargetPartitionMeta &target, bool distDeploy) {
    if (target.getIncVersion() == INVALID_VERSION) {
        SUEZ_PREFIX_LOG(INFO, "target inc version is %d, do not need deploy", INVALID_VERSION);
        return DS_DEPLOYDONE;
    }
    return SuezIndexPartition::doDeployIndex(target, distDeploy);
}

unique_ptr<IIndexlibAdapter> SuezTabletPartition::createIndexlibAdapter(const PartitionProperties &properties,
                                                                        uint64_t branchId) const {
    auto schema = loadSchema(properties);
    if (!schema) {
        return nullptr;
    }
    shared_ptr<indexlibv2::framework::ITabletMergeController> mergeController;
    if (!properties.mergeControllerConfig.mode.empty()) {
        mergeController = createMergeController(properties, schema, branchId);
        if (!mergeController) {
            return nullptr;
        }
    }
    std::string role = (properties.roleType == RT_LEADER) ? "leader" : "follower";
    kmonitor::MetricsTags tags("role_type", role);
    kmonitor::MetricsReporterPtr metricsReporter;
    if (_metricsReporter != nullptr) {
        metricsReporter = _metricsReporter->getSubReporter(/*empty sub path*/ "", tags);
    }
    indexlib::framework::TabletId tid(_pid.tableId.tableName, _pid.tableId.fullVersion, _pid.from, _pid.to);
    tid.SetIp(IpUtil::getHostAddress());
    auto tablet = indexlibv2::framework::TabletCreator()
                      .SetTabletId(tid)
                      .SetExecutor(_dumpExecutor, _taskScheduler)
                      .SetMemoryQuotaController(_globalIndexResource->GetMemoryQuotaController(),
                                                _globalIndexResource->GetRealtimeQuotaController())
                      .SetFileBlockCacheContainer(_globalIndexResource->GetFileBlockCacheContainer())
                      .SetSearchCache(_globalIndexResource->GetSearchCache())
                      .SetMetricsReporter(metricsReporter)
                      .SetMergeController(mergeController)
                      .CreateTablet();

    SUEZ_PREFIX_LOG(INFO, "create tablet with role: %s", RoleType_Name(properties.roleType));

    return make_unique<TabletAdapter>(
        move(tablet), properties.primaryIndexRoot, properties.secondaryIndexRoot, schema, properties.roleType);
}

TableBuilder *SuezTabletPartition::createTableBuilder(IIndexlibAdapter *indexlibAdapter,
                                                      const PartitionProperties &properties) const {
    if (properties.roleType == RT_FOLLOWER && properties.disableFollowerBuild) {
        SUEZ_PREFIX_LOG(INFO, "follower build is disabled");
        return new DummyTableBuilder();
    }

    build_service::proto::PartitionId pid;
    if (!getBsPartitionId(properties, pid)) {
        return nullptr;
    }
    build_service::workflow::RealtimeBuilderResource rtResource(
        _metricsReporter, nullptr, _swiftClientCreator, _globalBuilderThreadResource, properties.realtimeInfo);
    rtResource.executor = _executor;
    rtResource.taskScheduler2 = _taskScheduler;
    return indexlibAdapter->createBuilder(pid, rtResource, properties);
}

shared_ptr<indexlibv2::framework::ITabletMergeController>
SuezTabletPartition::createMergeController(const PartitionProperties &properties,
                                           const shared_ptr<indexlibv2::config::TabletSchema> &schema,
                                           uint64_t branchId) const {
    const auto &config = properties.mergeControllerConfig;
    if (config.mode == "local") {
        indexlibv2::table::LocalTabletMergeController::InitParam param;
        // TODO: use MERGE executor
        param.executor = _executor;
        param.schema = schema;
        param.options = make_shared<indexlibv2::config::TabletOptions>(properties.tabletOptions);
        param.partitionIndexRoot = properties.rawIndexRoot;
        param.metricProvider = make_shared<indexlib::util::MetricProvider>(_metricsReporter);
        // TODO: set memory quota
        auto controller = make_shared<indexlibv2::table::LocalTabletMergeController>();
        auto status = controller->Init(param);
        if (!status.IsOK()) {
            SUEZ_PREFIX_LOG(ERROR, "create local merge controller failed: %s", status.ToString().c_str());
            return nullptr;
        }
        return controller;
    } else if (config.mode == "remote") {
        build_service::proto::PartitionId pid;
        if (!getBsPartitionId(properties, pid)) {
            SUEZ_PREFIX_LOG(ERROR, "get partition id failed");
            return nullptr;
        }
        build_service::merge::RemoteTabletMergeController::InitParam param;
        param.branchId = branchId;
        param.executor = _executor;
        param.schema = schema;
        param.options = make_shared<indexlibv2::config::TabletOptions>(properties.tabletOptions);
        param.remotePartitionIndexRoot = properties.rawIndexRoot;
        param.generationId = _pid.getFullVersion();
        param.tableName = _pid.getTableName();
        param.rangeFrom = static_cast<uint16_t>(_pid.from);
        param.rangeTo = static_cast<uint16_t>(_pid.to);
        param.configPath = properties.remoteConfigPath;
        param.bsServerAddress = properties.realtimeInfo.getBsServerAddress();
        if (pid.buildid().has_datatable()) {
            param.dataTable = pid.buildid().datatable();
        }
        auto controller = make_shared<build_service::merge::RemoteTabletMergeController>();
        auto status = controller->Init(param);
        if (!status.IsOK()) {
            SUEZ_PREFIX_LOG(ERROR, "create remote merge controller failed: %s", status.ToString().c_str());
            return nullptr;
        }
        return controller;
    } else {
        assert(false);
        return nullptr;
    }
}

shared_ptr<indexlibv2::config::TabletSchema>
SuezTabletPartition::loadSchema(const PartitionProperties &properties) const {
    auto schemaDir = properties.schemaRoot;
    std::string schemaName = "schema.json";
    auto schemaFile = fslib::fs::FileSystem::joinFilePath(schemaDir, schemaName);
    auto ec = fslib::fs::FileSystem::isExist(schemaFile);
    if (ec != fslib::EC_TRUE && ec != fslib::EC_FALSE) {
        SUEZ_PREFIX_LOG(ERROR, "isExist(%s) failed", schemaFile.c_str());
        return nullptr;
    }
    if (ec != fslib::EC_TRUE) {
        // TODO(yonghao.fyh): refactor resource reader
        build_service::config::ResourceReader resourceReader(properties.localConfigPath);
        string tableName;
        if (!resourceReader.getClusterConfigWithJsonPath(
                properties.getTableName(), "cluster_config.table_name", tableName)) {
            tableName = properties.getTableName();
        }
        schemaFile = resourceReader.getSchemaConfRelativePath(tableName);
        schemaFile = fslib::fs::FileSystem::joinFilePath(properties.localConfigPath, schemaFile);
        schemaDir = indexlib::util::PathUtil::GetParentDirPath(schemaFile);
        schemaName = indexlib::util::PathUtil::GetFileName(schemaFile);
    }
    try {
        SUEZ_PREFIX_LOG(INFO, "start load schema, path [%s]", schemaFile.c_str());
        std::shared_ptr<indexlibv2::config::TabletSchema> schema =
            indexlibv2::framework::TabletSchemaLoader::LoadSchema(schemaDir, schemaName);
        if (!schema) {
            SUEZ_PREFIX_LOG(ERROR, "load schema from %s failed", schemaFile.c_str());
            return nullptr;
        }
        return schema;
    } catch (const exception &e) {
        SUEZ_PREFIX_LOG(ERROR, "load schema from %s failed, exception: %s", schemaFile.c_str(), e.what());
        return nullptr;
    }
}

std::shared_ptr<TableWriter> SuezTabletPartition::createTableWriter(const PartitionProperties &properties) const {
    build_service::proto::PartitionId pid;
    if (!getBsPartitionId(properties, pid)) {
        SUEZ_PREFIX_LOG(ERROR, "construct build service PartitionId failed");
        return nullptr;
    }
    auto walConfig = properties.walConfig;
    if (WALStrategy::needSink(walConfig.strategy) && !walConfig.hasSink()) {
        SUEZ_PREFIX_LOG(ERROR, "wal_config doesn't has sink ");
        return nullptr;
    }
    auto tableWriter = std::make_shared<TableWriter>();
    if (!tableWriter->init(pid, properties.localConfigPath, _metricsReporter, walConfig, _swiftClientCreator)) {
        SUEZ_PREFIX_LOG(ERROR, "init writer failed");
        return nullptr;
    }
    tableWriter->setEnableWrite(properties.roleType == RT_LEADER || properties.allowFollowerWrite);
    tableWriter->setRoleType(properties.roleType);
    return tableWriter;
}

bool SuezTabletPartition::initLogReplicator(IIndexlibAdapter *indexlibAdapter, const PartitionProperties &properties) {
    if (!properties.drcConfig.isEnabled()) {
        return true;
    }
    if (properties.roleType != RT_LEADER) {
        SUEZ_PREFIX_LOG(INFO, "only start drc in leader");
        return true;
    }
    {
        autil::ScopedLock lock(_mutex);
        if (_logReplicator) {
            return true;
        }
    }
    // TODO: merge data description to source config
    auto concreteAdapter = dynamic_cast<TabletAdapter *>(indexlibAdapter);
    if (!concreteAdapter) {
        SUEZ_PREFIX_LOG(ERROR, "only support drc in tablet");
        return false;
    }
    auto drcConfig = properties.drcConfig;
    auto &sourceConfig = drcConfig.getSourceConfig();
    // fill Source
    if (properties.realtimeInfo.isValid()) {
        const auto &kvMap = properties.realtimeInfo.getKvMap();
        for (const auto &it : kvMap) {
            sourceConfig.addParam(it.first, it.second);
        }
    }
    sourceConfig.addParam("from", to_string(_pid.from));
    sourceConfig.addParam("to", to_string(_pid.to));

    // fill checkpoint path
    if (drcConfig.getCheckpointRoot().empty()) {
        auto zkRoot = autil::EnvUtil::getEnv("zk_root", "");
        if (zkRoot.empty()) {
            SUEZ_PREFIX_LOG(ERROR, "no checkpoint root in drc config");
            return false;
        } else {
            auto partitionDir = TablePathDefine::constructPartitionDir(_pid);
            string checkpointRoot;
            if (zkRoot == "LOCAL") {
                checkpointRoot = PathDefine::join("./", partitionDir);
            } else {
                checkpointRoot = PathDefine::join(zkRoot, partitionDir);
            }
            SUEZ_PREFIX_LOG(INFO, "drc checkpoint root is: %s", checkpointRoot.c_str());
            drcConfig.setCheckpointRoot(move(checkpointRoot));
        }
    }

    auto tablet = concreteAdapter->getTablet();
    auto logReplicator =
        std::make_unique<LogReplicator>(drcConfig, _swiftClientCreator, std::move(tablet), _metricsReporter);
    if (!logReplicator->start()) {
        SUEZ_PREFIX_LOG(ERROR, "start log replicator failed");
        return false;
    }
    autil::ScopedLock lock(_mutex);
    _logReplicator = move(logReplicator);
    return true;
}

void SuezTabletPartition::closeLogReplicator() {
    autil::ScopedLock lock(_mutex);
    if (_logReplicator) {
        _logReplicator->stop();
    }
    _logReplicator.reset();
}

bool SuezTabletPartition::getBsPartitionId(const PartitionProperties &properties,
                                           build_service::proto::PartitionId &pid) const {
    SuezIndexPartition::getBsPartitionId(properties, pid);
    if (properties.realtimeInfo.needReadRawDoc()) {
        build_service::config::ResourceReader resourceReader(properties.localConfigPath);
        string dataTableName;
        if (!resourceReader.getDataTableFromClusterName(_pid.getTableName(), dataTableName)) {
            return false;
        }
        pid.mutable_buildid()->set_datatable(dataTableName);
    }
    return true;
}

} // namespace suez
