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
#include "suez/table/TableManager.h"

#include <algorithm>
#include <assert.h>
#include <functional>
#include <iosfwd>
#include <memory>
#include <stdlib.h>

#include "autil/EnvUtil.h"
#include "autil/Log.h"
#include "autil/MemUtil.h"
#include "autil/StringUtil.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/util/SwiftClientCreator.h"
#include "build_service/workflow/RealtimeBuilderDefine.h"
#include "fslib/fs/FileSystem.h"
#include "future_lite/ExecutorCreator.h"
#include "future_lite/TaskScheduler.h"
#include "indexlib/partition/partition_group_resource.h"
#include "kmonitor/client/MetricsReporter.h"
#include "suez/common/InnerDef.h"
#include "suez/common/TablePathDefine.h"
#include "suez/common/TargetLayout.h"
#include "suez/heartbeat/HeartbeatTarget.h"
#include "suez/sdk/CmdLineDefine.h"
#include "suez/sdk/PathDefine.h"
#include "suez/sdk/SourceReaderProvider.h"
#include "suez/sdk/TableReader.h"
#include "suez/table/Commit.h"
#include "suez/table/DecisionMakerChain.h"
#include "suez/table/SuezIndexPartition.h"
#include "suez/table/SuezPartitionFactory.h"

using namespace std;
using namespace autil;
using namespace kmonitor;
using namespace indexlib::partition;

namespace suez {

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, TableManager);

TableManager::TableManager() {
    _decisionMaker = std::make_unique<DecisionMakerChain>();
    _partitionFactory = std::make_unique<SuezPartitionFactory>();
}

TableManager::~TableManager() { stop(); }

uint64_t TableManager::getAllowedMem() const {
    uint64_t total = autil::MemUtil::getMachineTotalMem();
    uint64_t allow = total;
    uint64_t envMem = autil::EnvUtil::getEnv(RS_ALLOW_MEMORY, 0ul);
    if (envMem > 0) {
        allow = min(envMem, allow);
    }
    double envRatio = autil::EnvUtil::getEnv(RS_ALLOW_RATIO, (double)0.9);
    if (envRatio <= 0 || envRatio > 1.0) {
        AUTIL_LOG(WARN, "RS_ALLOW_RATIO is not valid[%lf], set to 0.9", envRatio);
        envRatio = 0.9;
    }
    allow = uint64_t(allow * envRatio);
    AUTIL_LOG(INFO,
              "index mem: total mem [%lu] MB, ratio [%lf], up limit [%lu] MB",
              total / 1024 / 1024,
              envRatio,
              allow / 1024 / 1024);
    return allow;
}

bool TableManager::init(const InitParam &param) {
    _todoListExecutor = std::make_unique<TodoListExecutor>();
    _todoListExecutor->addRunner(
        std::make_unique<AsyncRunner>(param.loadThreadPool, "load"),
        {OP_LOAD, OP_PRELOAD, OP_FORCELOAD, OP_RELOAD, OP_UNLOAD, OP_BECOME_LEADER, OP_NO_LONGER_LEADER, OP_COMMIT});
    _todoListExecutor->addRunner(std::make_unique<AsyncRunner>(param.deployThreadPool, "deploy"),
                                 {OP_INIT, OP_DEPLOY, OP_DIST_DEPLOY, OP_CLEAN_DISK, OP_CLEAN_INC_VERSION});

    _scheduleConfig.initFromEnv();
    AUTIL_LOG(INFO, "schedule config is: [%s]", _scheduleConfig.toString().c_str());

    if (!initGlobalTableResource(param)) {
        return false;
    }

    initIndexlibV2Resource();
    _localIndexRoot = PathDefine::getLocalIndexRoot();
    _localTableConfigBaseDir = PathDefine::getLocalTableConfigBaseDir();
    return true;
}

UPDATE_RESULT
TableManager::update(const HeartbeatTarget &target, const HeartbeatTarget &finalTarget, bool shutdownFlag, bool sync) {
    if (shutdownFlag) {
        for (const auto &it : _tableMap) {
            it.second->shutdown();
        }
    }
    TodoList todoList;
    UPDATE_RESULT ret = generateTodoList(target, finalTarget, todoList);

    if (enforceUnloadLoadOrder(target.getTableMetas(), todoList)) {
        if (ret != UR_ERROR) {
            AUTIL_LOG(INFO,
                      "UPDATE_RESULT change from %s to %s caused by unload load conflict",
                      enumToCStr(ret),
                      enumToCStr(UR_ERROR));
            ret = UR_ERROR;
        }
    }
    todoList.maybeOptimize();
    _scheduleLog.append(todoList);

    clearUnusedTables(todoList.getRemovedTables());

    auto currentMetas = getTableMetas();
    const auto &targetMetas = target.getTableMetas();
    const auto &finalMetas = finalTarget.getTableMetas();

    if (todoList.needCleanDisk()) {
        auto s = cleanDisk(currentMetas, targetMetas, finalMetas);
        if (s.hasError()) {
            AUTIL_LOG(ERROR, "error occur when clean disk");
            ret = UR_ERROR;
        } else if (!s.cleaned()) {
            AUTIL_LOG(ERROR, "disk reach quota but nothing could clean, need stop service");
            ret = UR_ERROR;
        } else {
            AUTIL_LOG(INFO, "clean success after disk reach quota");
        }
    } else if (finalTarget.cleanDisk()) {
        cleanDisk(currentMetas, targetMetas, finalMetas);
    }

    execTodoList(todoList, sync);

    if (_latestDoneSignature != target.getSignature()) {
        if (ret == UR_REACH_TARGET) {
            AUTIL_LOG(INFO, "reach_target[%s] update signature", target.getSignature().c_str());
            _latestDoneSignature = target.getSignature();
        } else if ("" != _latestDoneSignature) {
            AUTIL_LOG(INFO, "target has changed,reset latestDoneSignature[%s] to []", _latestDoneSignature.c_str());
            _latestDoneSignature = "";
        }
    } else if (ret != UR_REACH_TARGET) {
        AUTIL_LOG(WARN,
                  "target signature[%s] is not changed,but table status changed to [%s]",
                  _latestDoneSignature.c_str(),
                  enumToCStr(ret));
        // ret = UR_REACH_TARGET;
        // todo: fix
    }

    reportStatus();

    return ret;
}

UPDATE_RESULT
TableManager::generateTodoList(const HeartbeatTarget &target, const HeartbeatTarget &finalTarget, TodoList &todoList) {
    generateKeepList(target, false, todoList);
    generateRemoveList(target.getTableMetas(), finalTarget.getTableMetas(), todoList);

    UPDATE_RESULT ret = UR_NEED_RETRY;
    if (todoList.size() == 0 && targetReached(target)) {
        generateKeepList(finalTarget, true, todoList);
        ret = UR_REACH_TARGET;
    }

    generateRollingFreeOperations(target, todoList);
    generateRollingFreeOperations(finalTarget, todoList);
    generateCommitOperations(target.getTableMetas(), todoList);
    generateCleanIncVersionOperations(target, finalTarget, todoList);

    if (todoList.needStopService()) {
        ret = UR_ERROR;
    }
    return ret;
}

void TableManager::generateKeepList(const HeartbeatTarget &target, bool isFinal, TodoList &result) {
    const auto &tableMetas = target.getTableMetas();
    auto tables = collectTables(tableMetas);

    DecisionMakerChain::Context ctx;
    ctx.isFinal = isFinal;
    ctx.supportPreload = _scheduleConfig.allowPreload && isFinal && target.preload();
    ctx.config = _scheduleConfig;
    if (isFinal) {
        ctx.config.allowReloadByConfig = false;
        ctx.config.allowReloadByIndexRoot = false;
    }

    for (const auto &it : tableMetas) {
        const auto &pid = it.first;
        const auto &targetMeta = it.second;
        if (tables.count(pid) == 0) {
            AUTIL_LOG(WARN, "create table for %s failed, need retry", FastToJsonString(pid, true).c_str());
            continue;
        }
        auto table = tables[pid];
        const auto &currentMeta = table->getPartitionMeta();

        auto op = _decisionMaker->makeDecision(currentMeta, targetMeta, ctx);
        result.addOperation(table, op, targetMeta);
    }
}

void TableManager::generateRemoveList(const TableMetas &target, const TableMetas &finalTarget, TodoList &result) {
    for (const auto &it : _tableMap) {
        auto targetIt = target.find(it.first);
        if (targetIt != target.end()) {
            continue;
        }
        auto finalTargetIt = finalTarget.find(it.first);
        const auto &table = it.second;
        if (finalTargetIt == finalTarget.end() || table->getPartitionMeta().isTableInMemory()) {
            auto op = _decisionMaker->remove(table->getPartitionMeta());
            result.addOperation(table, op);
        }
    }
}

bool TableManager::targetReached(const HeartbeatTarget &target) const {
    for (const auto &it : target.getTableMetas()) {
        const auto &pid = it.first;
        const auto &targetMeta = it.second;
        auto table = getTable(pid);
        if (table.get() == nullptr) {
            return false;
        }
        if (!table->targetReached(targetMeta, _scheduleConfig.allowForceLoad)) {
            return false;
        }
    }
    return true;
}

void TableManager::generateRollingFreeOperations(const HeartbeatTarget &target, TodoList &result) {
    const auto &targetMetas = target.getTableMetas();
    auto tables = collectTables(targetMetas);
    for (const auto &it : targetMetas) {
        const auto &pid = it.first;
        const auto &targetMeta = it.second;
        auto table = getTable(pid);
        if (!table) {
            continue;
        }
        const auto &currentMeta = table->getPartitionMeta();
        if (targetMeta.getKeepCount() != currentMeta.getKeepCount()) {
            result.addOperation(table, OP_UPDATEKEEPCOUNT, targetMeta);
        }
        if (targetMeta.getConfigKeepCount() != currentMeta.getConfigKeepCount()) {
            result.addOperation(table, OP_UPDATE_CONFIG_KEEPCOUNT, targetMeta);
        }
        if (TableMeta::needUpdateRt(targetMeta.getTableMeta(), currentMeta.getTableMeta())) {
            result.addOperation(table, OP_UPDATERT, targetMeta);
        }
    }
}

void TableManager::generateCommitOperations(const TableMetas &tableMetas, TodoList &todoList) const {
    for (const auto &it : tableMetas) {
        const auto &pid = it.first;
        auto table = getTable(pid);
        if (table.get() == nullptr || table->getTableStatus() != TS_LOADED) {
            continue;
        }

        if (table->needCommit()) {
            auto resultHandler = [this](const PartitionId &pid, const std::pair<bool, TableVersion> &ret) mutable {
                const_cast<TableManager *>(this)->handleCommitResult(pid, ret);
            };
            todoList.addOperation(std::make_shared<Commit>(table, std::move(resultHandler)));
        }
    }
}

void TableManager::handleCommitResult(const PartitionId &pid, const std::pair<bool, TableVersion> &version) {
    // 用于支持ReadWriteTableManager的扩展实现
}

void TableManager::generateCleanIncVersionOperations(const HeartbeatTarget &target,
                                                     const HeartbeatTarget &finalTarget,
                                                     TodoList &todoList) {
    if (needCleanIncVersion() || todoList.needCleanDisk()) {
        auto currentMetas = getTableMetas();
        const auto &targetMetas = target.getTableMetas();
        const auto &finalMetas = finalTarget.getTableMetas();

        for (const auto &it : _tableMap) {
            const auto &pid = it.first;

            set<IncVersion> inUseIncVersions;
            const auto &table = it.second;
            auto currentIt = currentMetas.find(pid);
            auto targetIt = targetMetas.find(pid);
            auto finalIt = finalMetas.find(pid);

            if (currentIt != currentMetas.end()) {
                const auto &currentMeta = currentIt->second;
                inUseIncVersions.emplace(currentMeta.getIncVersion());
                auto needKeepVersions = currentMeta.getNeedKeepVersions();
                inUseIncVersions.insert(needKeepVersions.begin(), needKeepVersions.end());
            }
            if (targetIt != targetMetas.end()) {
                inUseIncVersions.emplace(targetIt->second.getIncVersion());
            }
            if (finalIt != finalMetas.end()) {
                inUseIncVersions.emplace(finalIt->second.getIncVersion());
            }

            todoList.addOperation(std::make_shared<CleanIncVersion>(table, inUseIncVersions));
        }
    }
}

static std::vector<TablePtr> findSameTables(const PartitionId &pid, const std::map<PartitionId, TablePtr> &tables) {
    std::vector<TablePtr> result;
    for (const auto &it : tables) {
        const auto &pid2 = it.first;
        if (pid2 != pid && pid2.getTableName() == pid.getTableName() && pid2.getFullVersion() != pid.getFullVersion()) {
            result.push_back(it.second);
        }
    }
    return result;
}

bool TableManager::enforceUnloadLoadOrder(const TableMetas &tableMetas, TodoList &todoList) const {
    bool hasUnloadLoadConflict = false;
    for (const auto &it : tableMetas) {
        const auto &pid = it.first;
        const auto &targetMeta = it.second;
        if (targetMeta.getTableLoadType() != TLT_UNLOAD_FIRST) {
            continue;
        }
        auto sameTables = findSameTables(pid, _tableMap);
        for (const auto &table : sameTables) {
            if (table->getPartitionMeta().isTableInMemory()) {
                if (todoList.remove(OP_LOAD, pid)) {
                    AUTIL_LOG(INFO,
                              "partition %s need unload first, it has other version %s in memory",
                              FastToJsonString(pid, true).c_str(),
                              FastToJsonString(table->getPid(), true).c_str());
                    hasUnloadLoadConflict = true;
                }
            }
        }
    }
    return hasUnloadLoadConflict;
}

void TableManager::execTodoList(const TodoList &todoList, bool sync) { _todoListExecutor->execute(todoList, sync); }

map<PartitionId, TablePtr> TableManager::collectTables(const TableMetas &tableMetas) {
    map<PartitionId, TablePtr> tables;
    for (const auto &it : tableMetas) {
        auto table = getOrCreateTable(it.first, it.second);
        if (table.get() != nullptr) {
            tables.emplace(it.first, std::move(table));
        }
    }
    return tables;
}

TablePtr TableManager::getOrCreateTable(const PartitionId &pid, const PartitionMeta &meta) {
    auto it = _tableMap.find(pid);
    if (it != _tableMap.end()) {
        return it->second;
    }
    return createTable(pid, meta);
}

TablePtr TableManager::createTable(const PartitionId &pid, const PartitionMeta &meta) {
    auto metricsReporter = createTableMetricsReporter(pid);
    TableResource tableResource(pid, metricsReporter, _globalIndexResource, _swiftClientCreator, _deployManager);
    tableResource.globalBuilderThreadResource = _globalBuilderThreadResource;
    tableResource.executor = _executor.get();
    tableResource.dumpExecutor = _dumpExecutor.get();
    tableResource.taskScheduler = _taskScheduler.get();
    tableResource.allowLoadUtilRtRecovered = _scheduleConfig.allowLoadUtilRtRecovered;
    auto table = std::make_shared<Table>(tableResource, _partitionFactory.get());
    table->setAllowFollowerWrite(_scheduleConfig.allowFollowerWrite);

    _tableMap.emplace(pid, table);
    return table;
}

void TableManager::removeTable(const PartitionId &pid) { _tableMap.erase(pid); }

TablePtr TableManager::getTable(const PartitionId &pid) const {
    auto it = _tableMap.find(pid);
    if (it != _tableMap.end()) {
        return it->second;
    }
    return TablePtr();
}

void TableManager::stop() { _tableMap.clear(); }

TableMetas TableManager::getTableMetas() const {
    TableMetas partMetas;
    for (const auto &it : _tableMap) {
        partMetas[it.first] = it.second->getPartitionMeta();
    }
    return partMetas;
}

CleanResult
TableManager::cleanDisk(const TableMetas &current, const TableMetas &target, const TableMetas &final) const {
    TargetLayout targetLayout;
    genTargetLayout(targetLayout, current);
    genTargetLayout(targetLayout, target);
    genTargetLayout(targetLayout, final);
    return targetLayout.sync();
}

void TableManager::genTargetLayout(TargetLayout &layout, const TableMetas &tableMetas) const {
    for (const auto &tableMetaKV : tableMetas) {
        const auto &pid = tableMetaKV.first;
        const auto &partitionMeta = tableMetaKV.second;
        layout.addTarget(_localIndexRoot.empty() ? PathDefine::getLocalIndexRoot() : _localIndexRoot,
                         pid.tableId.tableName,
                         TablePathDefine::getGenerationDir(pid),
                         partitionMeta.getKeepCount());
        if (!partitionMeta.getConfigPath().empty()) {
            layout.addTarget(_localTableConfigBaseDir.empty() ? PathDefine::getLocalTableConfigBaseDir()
                                                              : _localTableConfigBaseDir,
                             pid.tableId.tableName,
                             PathDefine::getVersionSuffix(partitionMeta.getConfigPath()),
                             std::max(partitionMeta.getConfigKeepCount(), partitionMeta.getKeepCount()));
        }
    }
}

void TableManager::clearUnusedTables(const std::set<PartitionId> &unusedSet) {
    for (const auto &pid : unusedSet) {
        removeTable(pid);
    }
}

bool TableManager::getTableReader(const HeartbeatTarget &target, MultiTableReader &tableReader) const {
    bool ret = true;
    tableReader.clear();
    for (const auto &it : target.getTableMetas()) {
        const auto &pid = it.first;
        const auto &meta = it.second;
        auto tit = _tableMap.find(pid);
        if (_tableMap.end() == tit) {
            AUTIL_LOG(DEBUG, "table not found, %s", FastToJsonString(pid, true).c_str());
            ret = false;
            continue;
        }
        auto data = tit->second->getSuezPartitionData(_scheduleConfig.allowForceLoad, meta);
        if (!data) {
            AUTIL_LOG(DEBUG, "table is not available, %s", FastToJsonString(pid, true).c_str());
            ret = false;
            continue;
        }
        auto singleReader = std::make_shared<SingleTableReader>(data);
        singleReader->setTableDefConfig(tit->second->getPartitionMeta().getTableDefConfig());
        tableReader.addTableReader(tit->second->getPid(), singleReader);
    }
    return ret;
}

bool TableManager::getTableWriters(const HeartbeatTarget &target,
                                   map<PartitionId, shared_ptr<TableWriter>> &tableWriters) const {
    bool ret = true;
    for (const auto &it : target.getTableMetas()) {
        const auto &pid = it.first;
        auto tit = _tableMap.find(pid);
        if (tit == _tableMap.end()) {
            ret = false;
            continue;
        }
        const auto &table = tit->second;
        auto writer = table->getTableWriter();
        if (writer) {
            tableWriters[table->getPid()] = writer;
        }
    }
    return ret;
}

bool TableManager::getTableConfigs(const HeartbeatTarget &target, SourceReaderProvider &sourceReader) const {
    bool ret = true;
    sourceReader.swiftClientCreator = _swiftClientCreator;
    for (const auto &it : target.getTableMetas()) {
        const auto &pid = it.first;
        auto tit = _tableMap.find(pid);
        if (tit == _tableMap.end()) {
            ret = false;
            continue;
        }
        auto partition = tit->second->getSuezPartition();
        if (!partition) {
            ret = false;
            continue;
        }
        PartitionSourceReaderConfig config;
        if (!partition->initPartitionSourceReaderConfig(config)) {
            // construct empty config
            config.pid = pid;
        }
        if (!sourceReader.addSourceConfig(std::move(config))) {
            ret = false;
        }
    }
    return ret;
}

MetricsReporterPtr TableManager::createGlobalMetricsReporter() const {
    MetricsReporterPtr metricsReporter;
    string metricsPrefix = _kmonMetaInfo.globalTableMetricsPrefix.empty() ? _kmonMetaInfo.metricsPrefix
                                                                          : _kmonMetaInfo.globalTableMetricsPrefix;
    if (metricsPrefix.empty()) {
        metricsReporter.reset(new MetricsReporter(_kmonMetaInfo.metricsPath, {_kmonMetaInfo.tagsMap}, "global_table_"));
    } else {
        metricsReporter.reset(
            new MetricsReporter(metricsPrefix, _kmonMetaInfo.metricsPath, {_kmonMetaInfo.tagsMap}, "global_table_"));
    }
    return metricsReporter;
}

kmonitor::MetricsReporterPtr TableManager::createTableMetricsReporter(const PartitionId &pid) const {
    map<string, string> tagsMap = _kmonMetaInfo.tagsMap;
    tagsMap["generation"] = std::to_string(pid.tableId.fullVersion);
    stringstream node;
    node << pid.from << '_' << pid.to;
    tagsMap["partition"] = node.str();
    tagsMap["table"] = pid.getTableName();
    MetricsReporterPtr metricsReporter;
    string metricsPrefix =
        _kmonMetaInfo.tableMetricsPrefix.empty() ? _kmonMetaInfo.metricsPrefix : _kmonMetaInfo.tableMetricsPrefix;
    if (metricsPrefix.empty()) {
        metricsReporter.reset(
            new MetricsReporter(_kmonMetaInfo.metricsPath, {tagsMap}, "table_" + pid.tableId.tableName));
    } else {
        metricsReporter.reset(
            new MetricsReporter(metricsPrefix, _kmonMetaInfo.metricsPath, {tagsMap}, "table_" + pid.tableId.tableName));
    }
    return metricsReporter;
}

void TableManager::initIndexlibV2Resource() {
    if (_executor.get() != nullptr && _taskScheduler.get() != nullptr) {
        return;
    }
    static constexpr uint32_t DEFAULT_EXECUTOR_THREAD_COUNT = 4;
    uint32_t threadCount = autil::EnvUtil::getEnv<uint32_t>("executor_thread_count", DEFAULT_EXECUTOR_THREAD_COUNT);
    _executor = future_lite::ExecutorCreator::Create(
        /*type*/ "async_io",
        future_lite::ExecutorCreator::Parameters().SetExecutorName("SuezWrite").SetThreadNum(threadCount));

    _taskScheduler = std::make_unique<future_lite::TaskScheduler>(_executor.get());

    uint32_t dumpThreadCount =
        autil::EnvUtil::getEnv<uint32_t>("dump_executor_thread_count", /*defaultValue*/ threadCount);
    _dumpExecutor = future_lite::ExecutorCreator::Create(
        /*type*/ "async_io",
        future_lite::ExecutorCreator::Parameters().SetExecutorName("SuezDump").SetThreadNum(dumpThreadCount));
}

bool TableManager::initGlobalTableResource(const InitParam &param) {
    _kmonMetaInfo = param.kmonMetaInfo;
    _deployManager = param.deployManager;

    _globalMetricsReporter = createGlobalMetricsReporter();

    _globalIndexResource =
        indexlib::partition::PartitionGroupResource::Create(getAllowedMem(),
                                                            _globalMetricsReporter,
                                                            autil::EnvUtil::getEnv("RS_BLOCK_CACHE").c_str(),
                                                            autil::EnvUtil::getEnv("RS_SEARCH_CACHE").c_str());
    if (!_globalIndexResource) {
        AUTIL_LOG(ERROR, "create global index resource failed");
        return false;
    }
    _swiftClientCreator.reset(new build_service::util::SwiftClientCreator(_globalMetricsReporter));

    uint32_t threadCount = autil::EnvUtil::getEnv<uint32_t>("SUEZ_BUILDER_SHARED_THREADS", 0);
    if (threadCount != 0) {
        _globalBuilderThreadResource.workflowThreadPool =
            std::make_shared<build_service::workflow::WorkflowThreadPool>(threadCount);
    }

    return true;
}

void TableManager::reportStatus() {
    for (const auto &it : _tableMap) {
        it.second->reportStatus();
    }
}

bool TableManager::needCleanIncVersion() {
    int64_t curTime = TimeUtility::currentTime();
    if (_scheduleConfig.allowFastCleanIncVersion && curTime > _nextCleanIncVersionTime) {
        _nextCleanIncVersionTime = curTime + int64_t(_scheduleConfig.fastCleanIncVersionIntervalInSec) * 1000 * 1000;
        return true;
    }
    return false;
}

} // namespace suez
