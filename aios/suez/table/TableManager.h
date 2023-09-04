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
#pragma once

#include <map>
#include <set>
#include <stdint.h>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "build_service/workflow/RealtimeBuilderDefine.h"
#include "suez/common/InitParam.h"
#include "suez/common/InnerDef.h"
#include "suez/common/TableMeta.h"
#include "suez/sdk/KMonitorMetaInfo.h"
#include "suez/sdk/SuezError.h"
#include "suez/table/OperationType.h"
#include "suez/table/ScheduleLog.h"
#include "suez/table/SuezPartition.h"
#include "suez/table/Table.h"
#include "suez/table/TodoList.h"
#include "suez/table/TodoListExecutor.h"

namespace future_lite {
class Executor;
class TaskScheduler;
} // namespace future_lite

namespace suez {

class HeartbeatTarget;
class MultiTableReader;
class TargetLayout;

class DecisionMakerChain;
class SuezPartitionFactory;
class DeployManager;
class TableWriter;
class SourceReaderProvider;

class CleanResult;

class TableManager {
public:
    TableManager();
    virtual ~TableManager();

private:
    TableManager(const TableManager &);
    TableManager &operator=(const TableManager &);

public:
    virtual bool init(const InitParam &param);
    void stop();
    UPDATE_RESULT update(const HeartbeatTarget &target,
                         const HeartbeatTarget &finalTarget,
                         bool shutdownFlag = false,
                         bool sync = false);
    TableMetas getTableMetas() const;
    TablePtr getTable(const PartitionId &pid) const;
    bool getTableReader(const HeartbeatTarget &target, MultiTableReader &tableReader) const;
    bool getTableWriters(const HeartbeatTarget &target,
                         std::map<PartitionId, std::shared_ptr<TableWriter>> &tableWriters) const;
    bool getTableConfigs(const HeartbeatTarget &target, SourceReaderProvider &sourceReader) const;
    virtual void processTableIndications(const std::set<PartitionId> &releaseLeaderTables,
                                         const std::set<PartitionId> &campaginLeaderTables,
                                         const std::set<PartitionId> &publishLeaderTables,
                                         const std::map<PartitionId, int32_t> &tablesWeight) {}

protected:
    virtual UPDATE_RESULT
    generateTodoList(const HeartbeatTarget &target, const HeartbeatTarget &finalTarget, TodoList &todoList);
    virtual TablePtr createTable(const PartitionId &pid, const PartitionMeta &meta);
    virtual void removeTable(const PartitionId &pid);
    uint64_t getAllowedMem() const;
    void generateCleanIncVersionOperations(const HeartbeatTarget &target,
                                           const HeartbeatTarget &finalTarget,
                                           TodoList &todoList);
    virtual void handleCommitResult(const PartitionId &pid, const std::pair<bool, TableVersion> &version);

private:
    bool enforceUnloadLoadOrder(const TableMetas &tableMetas, TodoList &todoList) const;
    void execTodoList(const TodoList &todoList, bool sync = false);

    CleanResult cleanDisk(const TableMetas &current, const TableMetas &target, const TableMetas &finalTarget) const;

    void clearUnusedTables(const std::set<PartitionId> &unusedSet);

    void generateKeepList(const HeartbeatTarget &target, bool isFinal, TodoList &result);
    void generateRemoveList(const TableMetas &target, const TableMetas &finalTarget, TodoList &result);
    void generateRollingFreeOperations(const HeartbeatTarget &target, TodoList &result);
    void generateCommitOperations(const TableMetas &tableMetas, TodoList &todoList) const;
    bool targetReached(const HeartbeatTarget &target) const;

    std::map<PartitionId, TablePtr> collectTables(const TableMetas &tableMetas);
    TablePtr getOrCreateTable(const PartitionId &pid, const PartitionMeta &meta);

    kmonitor::MetricsReporterPtr createGlobalMetricsReporter() const;
    kmonitor::MetricsReporterPtr createTableMetricsReporter(const PartitionId &pid) const;

    void genTargetLayout(TargetLayout &layout, const TableMetas &tableMetas) const;

    void initIndexlibV2Resource();
    bool initGlobalTableResource(const InitParam &param);
    void reportStatus();
    bool needCleanIncVersion();

protected:
    KMonitorMetaInfo _kmonMetaInfo;
    kmonitor::MetricsReporterPtr _globalMetricsReporter;
    indexlib::partition::PartitionGroupResourcePtr _globalIndexResource;
    build_service::workflow::BuildFlowThreadResource _globalBuilderThreadResource;
    build_service::util::SwiftClientCreatorPtr _swiftClientCreator;

    std::map<PartitionId, TablePtr> _tableMap;
    std::unique_ptr<TodoListExecutor> _todoListExecutor;
    DeployManager *_deployManager;
    ScheduleConfig _scheduleConfig;

    ScheduleLog _scheduleLog;
    std::string _latestDoneSignature;
    int64_t _nextCleanIncVersionTime = -1;
    std::unique_ptr<DecisionMakerChain> _decisionMaker;
    std::unique_ptr<SuezPartitionFactory> _partitionFactory;

    // resource for indexlibv2
    std::unique_ptr<future_lite::Executor> _executor;
    std::unique_ptr<future_lite::Executor> _dumpExecutor;
    std::unique_ptr<future_lite::TaskScheduler> _taskScheduler;

    std::string _localIndexRoot;
    std::string _localTableConfigBaseDir;
};

} // namespace suez
