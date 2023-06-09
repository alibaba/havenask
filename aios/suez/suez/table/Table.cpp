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
#include "suez/table/Table.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "autil/EnvUtil.h"
#include "autil/Log.h"
#include "autil/Scope.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/indexlib.h"
#include "kmonitor/client/MetricsReporter.h"
#include "suez/sdk/PathDefine.h"
#include "suez/sdk/SuezError.h"
#include "suez/sdk/TableWriter.h"
#include "suez/table/LeaderElectionManager.h"
#include "suez/table/SuezPartitionFactory.h"
#include "suez/table/TableMeta.h"
#include "suez/table/TablePathDefine.h"

namespace suez {
class DataOptionWrapper;
} // namespace suez

using namespace std;
using namespace indexlib;
using namespace kmonitor;
using namespace indexlib::config;

namespace suez {

class DeployStatusMetrics : public MetricsGroup {
public:
    bool init(MetricsGroupManager *manager) override {
        REGISTER_STATUS_MUTABLE_METRIC(_deployStatusConfigError, "SuezDeployStatus.ConfigError");
        REGISTER_STATUS_MUTABLE_METRIC(_deployStatusIndexError, "SuezDeployStatus.IndexError");
        REGISTER_STATUS_MUTABLE_METRIC(_deployStatusDiskQuotaError, "SuezDeployStatus.DiskQuotaError");
        return true;
    }
    void report(const kmonitor::MetricsTags *tags, const SuezError *suezError) {
        auto errCode = suezError->errCode;
        REPORT_MUTABLE_METRIC(_deployStatusConfigError, errCode == DEPLOY_CONFIG_ERROR.errCode ? 1 : 0);
        REPORT_MUTABLE_METRIC(_deployStatusIndexError, errCode == DEPLOY_INDEX_ERROR.errCode ? 1 : 0);
        REPORT_MUTABLE_METRIC(_deployStatusDiskQuotaError, errCode == DEPLOY_DISK_QUOTA_ERROR.errCode ? 1 : 0);
    }

private:
    MutableMetric *_deployStatusConfigError = nullptr;
    MutableMetric *_deployStatusIndexError = nullptr;
    MutableMetric *_deployStatusDiskQuotaError = nullptr;
};

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, Table);

struct Table::SwitchState {
    SuezPartitionPtr partition;
    CurrentPartitionMetaPtr meta;
    std::atomic<bool> ready; // can accept read request ?
};

#define SUEZ_PREFIX ToJsonString(_tableResource.pid, true).c_str()

Table::Table(const TableResource &tableResource) : Table(tableResource, nullptr) {}

Table::Table(const TableResource &tableResource, SuezPartitionFactory *factory)
    : _tableResource(tableResource), _partitionMeta(new CurrentPartitionMeta) {
    if (factory != nullptr) {
        _factory = factory;
        _ownFactory = false;
    } else {
        _factory = new SuezPartitionFactory;
        _ownFactory = true;
    }
}

Table::~Table() {
    if (_ownFactory) {
        delete _factory;
        _factory = nullptr;
    }
}

static std::string constructSchemaFile(const std::string &partitionDir, SchemaVersion version) {
    std::string schemaFile = "schema.json";
    if (version > 0) {
        schemaFile += "." + std::to_string(version);
    }
    return PathDefine::join(partitionDir, schemaFile);
}

SuezPartitionType Table::determineSuezPartitionType(const TargetPartitionMeta &target) const {
    SuezPartitionType type = target.getTableType();
    if (SPT_NONE != type) {
        return type;
    }
    string partitionDir = TablePathDefine::constructIndexPath(target.getRawIndexRoot(), _tableResource.pid);
    auto schemaFile = constructSchemaFile(partitionDir, target.getSchemaVersion());
    try {
        std::string jsonStr;
        if (!indexlib::file_system::FslibWrapper::AtomicLoad(schemaFile, jsonStr).OK()) {
            SUEZ_PREFIX_LOG(ERROR, "load schema from %s failed", schemaFile.c_str());
            return SPT_NONE;
        }
        auto any = autil::legacy::json::ParseJson(jsonStr);
        auto jsonMap = autil::legacy::AnyCast<autil::legacy::json::JsonMap>(any);
        bool isLegacySchema = indexlibv2::config::TabletSchema::IsLegacySchema(jsonMap);
        if (!isLegacySchema) {
            return SPT_TABLET;
        }

        auto schema = indexlib::config::IndexPartitionSchema::LoadFromJsonStr(jsonStr, /*loadFromIndex*/ true);
        if (!schema) {
            SUEZ_PREFIX_LOG(ERROR, "load schema from %s failed", schemaFile.c_str());
            return SPT_NONE;
        }
        if (schema->GetTableType() == tt_rawfile || schema->GetTableType() == tt_linedata) {
            return SPT_RAWFILE;
        } else {
            const auto &userDefinedParam = schema->GetUserDefinedParam();
            auto itr = userDefinedParam.find("tablet");
            if (userDefinedParam.end() != itr && autil::legacy::AnyCast<bool>(itr->second)) {
                return SPT_TABLET;
            }
            if (autil::EnvUtil::getEnv("force_tablet_load", false)) {
                return SPT_TABLET;
            }
            return schema->IsTablet() ? SPT_TABLET : SPT_INDEXLIB;
        }
    } catch (const exception &e) {
        SUEZ_PREFIX_LOG(ERROR, "[%s] exception happened, schema file: %s", e.what(), schemaFile.c_str());
    } catch (...) { SUEZ_PREFIX_LOG(ERROR, "known exception happened"); }
    return SPT_NONE;
}

const PartitionId &Table::getPid() const { return _tableResource.pid; }

std::string Table::getIdentifier() const { return ToJsonString(_tableResource.pid, true); }

#define OP_LOG(op)                                                                                                     \
    SUEZ_PREFIX_LOG(INFO, "%s begin", op);                                                                             \
    autil::ScopedTime2 opTime;                                                                                         \
    auto end = [&]() {                                                                                                 \
        auto latency = opTime.done_us();                                                                               \
        SUEZ_PREFIX_LOG(INFO, "%s end, used time: %ld us", op, latency);                                               \
        reportLatency(op, latency);                                                                                    \
    };                                                                                                                 \
    autil::ScopeGuard logGuard(std::move(end))

void Table::init(const TargetPartitionMeta &target) {
    OP_LOG("init");
    setTableStatus(TS_INITIALIZING);
    SuezPartitionType type = determineSuezPartitionType(target);
    if (target.getTableMode() == TM_READ_WRITE) {
        if (type == SPT_INDEXLIB) {
            type = SPT_TABLET;
        }
        if (type != SPT_TABLET) {
            SUEZ_PREFIX_LOG(ERROR, "only SPT_TABLET support write, actual: %d", int(type));
            updateTableStatus(StatusAndError<TableStatus>(TS_UNKNOWN, CREATE_TABLE_ERROR));
            return;
        }
    }

    if (_tableResource.pid.index < 0) {
        _tableResource.pid.setPartitionIndex(target.getTotalPartitionCount());
    }

    _suezPartition = _factory->create(type, _tableResource, _partitionMeta);
    // TODO can set TableMeta ?
    if (!_suezPartition) {
        updateTableStatus(StatusAndError<TableStatus>(TS_UNKNOWN, CREATE_TABLE_ERROR));
    } else {
        _partitionMeta->setTableType(type);
        _partitionMeta->setTotalPartitionCount(target.getTotalPartitionCount());
        _partitionMeta->setTableMode(target.getTableMode());
        updateTableStatus(StatusAndError<TableStatus>(TS_UNLOADED, ERROR_NONE));
    }
}

#define MAYBE_WAIT_ON_ROLE_SWITCHING()                                                                                 \
    do {                                                                                                               \
        autil::ScopedLock lock(_switchMutex);                                                                          \
        if (_partitionMeta->getTableStatus() == TS_ROLE_SWITCHING) {                                                   \
            return;                                                                                                    \
        }                                                                                                              \
    } while (0)

void Table::deploy(const TargetPartitionMeta &target, bool distDeploy) {
    MAYBE_WAIT_ON_ROLE_SWITCHING();
    OP_LOG("deploy");
    autil::ScopedTime2 timeGuard;
    SuezPartitionPtr suezPartition;
    CurrentPartitionMetaPtr meta;
    {
        autil::ScopedLock lock(_switchMutex);
        suezPartition = _suezPartition;
        meta = _partitionMeta;
    }
    meta->setDeployStatus(target.getIncVersion(), DS_DEPLOYING);
    auto status = suezPartition->deploy(target, distDeploy);
    meta->setDeployStatus(target.getIncVersion(), status.status);
    meta->setError(status.error);
    flushDeployStatusMetric();
    if (status.status == DS_DEPLOYDONE && _leaderElectionMgr != nullptr) {
        AUTIL_LOG(INFO, "[%s] deploy done, allow to campagin leader.", ToJsonString(getPid(), true).c_str());
        _leaderElectionMgr->allowCampaginLeader(getPid());
    }
    AUTIL_LOG(INFO,
              "deploy table:[%s] with incVersion:[%d], configPath:[%s], indexRoot:[%s], "
              "rawIndexRoot:[%s], distDeploy:[%s], deployStatus:[%s] and time:[%.2lf]s",
              ToJsonString(getPid(), true).c_str(),
              target.getIncVersion(),
              target.getConfigPath().c_str(),
              target.getIndexRoot().c_str(),
              target.getRawIndexRoot().c_str(),
              distDeploy ? "true" : "false",
              enumToCStr(status.status),
              timeGuard.done_sec());
}

void Table::cancelDeploy() {
    MAYBE_WAIT_ON_ROLE_SWITCHING();
    OP_LOG("cancelDeploy");
    if (_suezPartition) {
        _suezPartition->cancelDeploy();
    }
}

void Table::cancelLoad() {
    MAYBE_WAIT_ON_ROLE_SWITCHING();
    OP_LOG("cancelLoad");
    if (_suezPartition) {
        _suezPartition->cancelLoad();
    }
}

void Table::load(const TargetPartitionMeta &target) {
    MAYBE_WAIT_ON_ROLE_SWITCHING();
    OP_LOG("load");
    setTableStatus(TS_LOADING);
    auto ret = _suezPartition->load(target, false);
    updateTableStatus(ret);
    if (ret.status == TS_LOADED) {
        _partitionMeta->setRoleType(target.getRoleType());
    }
    if (_leaderInfoPublisher) {
        _leaderInfoPublisher->updateLeaderInfo();
    }
}

void Table::forceLoad(const TargetPartitionMeta &target) {
    MAYBE_WAIT_ON_ROLE_SWITCHING();
    OP_LOG("forceLoad");
    if (_suezPartition->isInUse()) {
        AUTIL_LOG(INFO,
                  "force load: wait table [%s] released by service. ref count > 1",
                  ToJsonString(getPid(), true).c_str());
        return;
    }
    setTableStatus(TS_FORCELOADING);
    auto ret = _suezPartition->load(target, true);
    updateTableStatus(ret);
    if (ret.status == TS_LOADED) {
        _partitionMeta->setRoleType(target.getRoleType());
    }
}

void Table::reload(const TargetPartitionMeta &target) {
    MAYBE_WAIT_ON_ROLE_SWITCHING();
    OP_LOG("reload");
    if (tryReload(target)) {
        _partitionMeta->setRoleType(target.getRoleType());
    }
}

void Table::preload(const TargetPartitionMeta &target) {
    MAYBE_WAIT_ON_ROLE_SWITCHING();
    OP_LOG("preload");
    setTableStatus(TS_PRELOADING);
    auto ret = _suezPartition->load(target, false);
    // rewrite status for preload
    // TODO: remove these special status to simplify the state machine
    if (ret.status != TS_LOADED) {
        if (ret.status == TS_FORCE_RELOAD) {
            ret.status = TS_PRELOAD_FORCE_RELOAD;
        } else {
            ret.status = TS_PRELOAD_FAILED;
        }
    }
    updateTableStatus(ret);
}

void Table::unload() {
    MAYBE_WAIT_ON_ROLE_SWITCHING();
    OP_LOG("unload");
    if (_suezPartition && _suezPartition->isInUse()) {
        AUTIL_LOG(INFO,
                  "do unload: wait table [%s] released by service. ref count > 1",
                  autil::legacy::ToJsonString(getPid(), true).c_str());
        return;
    }
    setTableStatus(TS_UNLOADING);
    if (_suezPartition) {
        _suezPartition->unload();
    }
    setTableStatus(TS_UNLOADED);
}

void Table::updateRt(const TargetPartitionMeta &target) {
    MAYBE_WAIT_ON_ROLE_SWITCHING();
    OP_LOG("updateRt");
    if (!_suezPartition) {
        return;
    }
    TableRtStatus rtStatus = target.getRtStatus();
    if (TRS_BUILDING == rtStatus) {
        _suezPartition->resumeRt();
    } else if (TRS_SUSPENDED == rtStatus) {
        _suezPartition->suspendRt();
    }
    if (target.getForceOnline()) {
        _suezPartition->forceOnline();
    }
    if (target.getTimestampToSkip() != -1) {
        _suezPartition->setTimestampToSkip(target.getTimestampToSkip());
    }
}

void Table::setKeepCount(size_t keepCount) {
    MAYBE_WAIT_ON_ROLE_SWITCHING();
    autil::ScopedLock lock(_switchMutex);
    _partitionMeta->setKeepCount(keepCount);
}

void Table::setConfigKeepCount(size_t configKeepCount) {
    MAYBE_WAIT_ON_ROLE_SWITCHING();
    autil::ScopedLock lock(_switchMutex);
    _partitionMeta->setConfigKeepCount(configKeepCount);
}

void Table::finalTargetToTarget() {
    MAYBE_WAIT_ON_ROLE_SWITCHING();
    auto ts = getTableStatus();
    switch (ts) {
    case TS_PRELOADING:
        setTableStatus(TS_LOADING);
        break;
    case TS_PRELOAD_FAILED:
        setTableStatus(TS_ERROR_UNKNOWN);
        break;
    case TS_PRELOAD_FORCE_RELOAD:
        setTableStatus(TS_FORCE_RELOAD);
        break;
    default:
        break;
    }
}

bool Table::needCommit() const {
    {
        autil::ScopedLock lock(_switchMutex);
        if (_partitionMeta->getTableStatus() == TS_ROLE_SWITCHING) {
            return false;
        }
    }
    if (_suezPartition.get() != nullptr) {
        return _suezPartition->needCommit();
    }
    return false;
}

std::pair<bool, TableVersion> Table::commit() {
    OP_LOG("commit");
    setTableStatus(TS_COMMITTING);
    auto ret = _suezPartition->commit();
    if (ret.first) {
        ret.second.setIsLeaderVersion(_partitionMeta->getRoleType() == RT_LEADER);
    }
    setTableStatus(ret.first ? TS_LOADED : TS_COMMIT_ERROR);
    return ret;
}

void Table::doRoleSwitch(const TargetPartitionMeta &target) {
    std::shared_ptr<SwitchState> state;
    {
        autil::ScopedLock lock(_switchMutex);
        _partitionMeta->setTableStatus(TS_ROLE_SWITCHING);
        auto writer = _suezPartition->getTableWriter();
        if (writer) {
            writer->updateIndex(nullptr);
            // TODO: maybe update writer version
            // 如果允许follower write，正确的做法还是得支持swift partition锁
            writer->setEnableWrite(_allowFollowerWrite || target.getRoleType() == RT_LEADER);
            writer->setRoleType(target.getRoleType());
            _suezPartition->setTableWriter(nullptr);
        }
        _suezPartition->stopWrite(SuezPartition::SWO_DRC);
        state = std::make_shared<SwitchState>();
        state->meta = std::make_shared<CurrentPartitionMeta>(*_partitionMeta);
        state->partition = _factory->create(state->meta->getTableType(), _tableResource, state->meta);
        state->ready = false;
        if (!state->partition) {
            SUEZ_PREFIX_LOG(ERROR, "create partition failed");
            _partitionMeta->setTableStatus(TS_ROLE_SWITCH_ERROR);
            return;
        }
        if (writer) {
            state->partition->setTableWriter(writer);
        }
        _switchState = state;
    }

    assert(state);

    SUEZ_PREFIX_LOG(DEBUG, "load switch state partition begin");
    auto ret = state->partition->load(target, false);
    if (ret.status != TS_LOADED) {
        SUEZ_PREFIX_LOG(WARN, "load target partition failed, need reload");
        // TODO: 快速leader切换（比如内存不够）还是会触发reload，摘流，不安全，需要admin介入兜底
        state->partition->stopWrite();
        autil::ScopedLock lock(_switchMutex);
        _switchState.reset();
        _partitionMeta->setTableStatus(TS_ROLE_SWITCH_ERROR);
        return;
    }
    SUEZ_PREFIX_LOG(DEBUG, "load switch state partition end");

    static constexpr int64_t CHECK_INTERVAL_US = 5000; // 5ms
    while (!state->partition->isRecovered()) {
        usleep(CHECK_INTERVAL_US);
        AUTIL_INTERVAL_LOG2(10,
                            INFO,
                            "[%p,%s]: waiting partition [%p] rt build recovered",
                            this,
                            ToJsonString(getPid(), true).c_str(),
                            _suezPartition.get());
    }
    state->ready = true;

    while (_suezPartition->isInUse()) {
        usleep(CHECK_INTERVAL_US);
        AUTIL_INTERVAL_LOG2(10,
                            INFO,
                            "[%p,%s]: waiting old partition [%p] release, use count[%d]",
                            this,
                            ToJsonString(getPid(), true).c_str(),
                            _suezPartition.get(),
                            _suezPartition->getUseCount());
    }
    {
        autil::ScopedLock lock(_switchMutex);
        SUEZ_PREFIX_LOG(DEBUG, "unload old partition begin");
        _suezPartition->unload();
        _suezPartition = _switchState->partition;
        _partitionMeta = _switchState->meta;
        _partitionMeta->setTableStatus(TS_LOADED);
        _partitionMeta->setRoleType(target.getRoleType());
        _switchState.reset();
        SUEZ_PREFIX_LOG(DEBUG, "unload old partition end");
    }
    flushDeployStatusMetric();
}

void Table::becomeLeader(const TargetPartitionMeta &target) {
    OP_LOG("becomeLeader");
    doRoleSwitch(target);
}

void Table::nolongerLeader(const TargetPartitionMeta &target) {
    OP_LOG("nolongerLeader");
    doRoleSwitch(target);
}

void Table::setTableStatus(TableStatus status) {
    autil::ScopedLock lock(_switchMutex);
    _partitionMeta->setTableStatus(status);
}

TableStatus Table::getTableStatus() const {
    autil::ScopedLock lock(_switchMutex);
    return _partitionMeta->getTableStatus();
}

DeployStatus Table::getTableDeployStatus(IncVersion incVersion) const {
    autil::ScopedLock lock(_switchMutex);
    return _partitionMeta->getDeployStatus(incVersion);
}

CurrentPartitionMeta Table::getPartitionMeta() const {
    autil::ScopedLock lock(_switchMutex);
    return *_partitionMeta;
}

SuezPartitionPtr Table::getSuezPartition() const {
    autil::ScopedLock lock(_switchMutex);
    if (_switchState && _switchState->ready) {
        return _switchState->partition;
    }
    return _suezPartition;
}

SuezPartitionDataPtr Table::getSuezPartitionData(bool allowForceLoad, const TargetPartitionMeta &target) const {
    autil::ScopedLock lock(_switchMutex);
    if (!isAvailable(allowForceLoad, target)) {
        return nullptr;
    }
    if (_switchState && _switchState->ready) {
        return _switchState->partition->getPartitionData();
    } else {
        return _suezPartition->getPartitionData();
    }
}

std::shared_ptr<TableWriter> Table::getTableWriter() const {
    autil::ScopedLock lock(_switchMutex);
    std::shared_ptr<TableWriter> writer;
    if (_switchState && _switchState->partition) {
        writer = _switchState->partition->getTableWriter();
    }
    if (!writer && _suezPartition) {
        writer = _suezPartition->getTableWriter();
    }
    return writer;
}

void Table::shutdown() {
    MAYBE_WAIT_ON_ROLE_SWITCHING();
    if (!_suezPartition) {
        SUEZ_PREFIX_LOG(DEBUG, "suezPartition is not inited, not need shutdown.");
        return;
    }
    _suezPartition->shutdown();
}

bool Table::targetReached(const TargetPartitionMeta &target, bool allowForceLoad) const {
    auto current = getPartitionMeta();
    return targetReached(target, current, allowForceLoad);
}

bool Table::targetReached(const TargetPartitionMeta &target, const CurrentPartitionMeta &current, bool allowForceLoad) {
    auto dpStatus = current.getDeployStatus(target.getIncVersion());
    return dpStatus == DS_DEPLOYDONE && current.isReady(allowForceLoad) &&
           (current.getLoadedConfigPath() == target.getConfigPath() || current.getLoadedConfigPath().empty()) &&
           (current.getLoadedIndexRoot() == target.getIndexRoot() || current.getLoadedIndexRoot().empty());
}

bool Table::isAvailable(bool allowForceLoad, const TargetPartitionMeta &target) const {
    if (!_suezPartition) {
        SUEZ_PREFIX_LOG(DEBUG, "suezPartition is not inited, not available now.");
        return false;
    }
    if (!isRecovered()) {
        SUEZ_PREFIX_LOG(DEBUG, "suezPartition is not recovered, not available now.");
        return false;
    }
    auto type = _partitionMeta->getTableType();
    if (type != SPT_INDEXLIB && type != SPT_TABLET) {
        return _partitionMeta->getTableStatus() == TS_LOADED;
    } else {
        return _partitionMeta->isAvailable(allowForceLoad, target, getPid());
    }
}

void Table::setLeaderElectionManager(LeaderElectionManager *leaderElectionMgr) {
    _leaderElectionMgr = leaderElectionMgr;
}

void Table::setLeaderInfoPublisher(const std::shared_ptr<TableLeaderInfoPublisher> &leaderInfoPublisher) {
    _leaderInfoPublisher = leaderInfoPublisher;
}

void Table::setAllowFollowerWrite(bool allowFollowerWrite) { _allowFollowerWrite = allowFollowerWrite; }

void Table::updateTableStatus(const StatusAndError<TableStatus> &status) {
    _partitionMeta->setTableStatus(status.status);
    _partitionMeta->setError(status.error);
}

void Table::cleanIncVersion(const std::set<IncVersion> &inUseVersions) {
    MAYBE_WAIT_ON_ROLE_SWITCHING();
    OP_LOG("cleanIncVersion");

    SuezPartitionPtr partition;
    {
        autil::ScopedLock lock(_switchMutex);
        partition = _suezPartition;
    }

    AUTIL_LOG(INFO, "keep versions [%s]", autil::StringUtil::toString(inUseVersions, ",").c_str());

    _partitionMeta->clearIncVersion(inUseVersions);

    if (partition) {
        partition->cleanIndexFiles(inUseVersions);
    }
}

bool Table::tryReload(const TargetPartitionMeta &target) {
    if (_suezPartition->isInUse()) {
        AUTIL_LOG(ERROR,
                  "reload: wait table [%p,%s] released by service. ref count > 1, do nothing.",
                  this,
                  ToJsonString(getPid(), true).c_str());
        return false;
    }
    setTableStatus(TS_FORCELOADING);
    _suezPartition->unload();
    auto ret = _suezPartition->load(target, true);
    updateTableStatus(ret);
    return ret.status == TS_LOADED;
}

void Table::reportLatency(const std::string &op, int64_t latency) {
    if (!_tableResource.metricsReporter) {
        return;
    }
    auto metricName = op + ".latency";
    _tableResource.metricsReporter->report(latency, metricName, kmonitor::GAUGE, nullptr);
}

void Table::reportStatus() {
    auto current = getPartitionMeta();
    if (TM_READ_WRITE != current.getTableMode()) {
        return;
    }

    auto &reporter = _tableResource.metricsReporter;
    if (reporter) {
        bool isLeader = _leaderElectionMgr && _leaderElectionMgr->isLeader(getPid());
        REPORT_USER_MUTABLE_STATUS(reporter, "leader", (isLeader ? 1 : 0));
    }
    if (_leaderElectionMgr != nullptr && current.getLatestDeployStatus() == DS_DEPLOYDONE) {
        _leaderElectionMgr->allowCampaginLeader(getPid());
    }
    if (_leaderInfoPublisher) {
        _leaderInfoPublisher->updateLeaderInfo();
    }
}

void Table::flushDeployStatusMetric() const {
    auto *metricsReporter = _tableResource.metricsReporter.get();
    if (metricsReporter) {
        // ATTENTION: report metric under lock
        autil::ScopedLock lock(_switchMutex);
        metricsReporter->report<DeployStatusMetrics>(nullptr, &_partitionMeta->getError());
    }
}

bool Table::isRecovered() const {
    if (_suezPartition) {
        return _suezPartition->isRecovered();
    }
    return true;
}

#undef OP_LOG

} // namespace suez
