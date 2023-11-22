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
#include "build_service_tasks/rollback/RollbackTask.h"

#include <iosfwd>
#include <map>
#include <memory>
#include <vector>

#include "alog/Logger.h"
#include "autil/Span.h"
#include "autil/StringUtil.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "autil/legacy/legacy_jsonizable_dec.h"
#include "build_service/config/BuildRuleConfig.h"
#include "build_service/config/BuildServiceConfig.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/CounterConfig.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/ErrorCollector.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/util/ErrorLogCollector.h"
#include "build_service/util/IndexPathConstructor.h"
#include "build_service/util/RangeUtil.h"
#include "indexlib/partition/index_roll_back_util.h"

using namespace std;
using namespace build_service::config;
using namespace build_service::proto;
using namespace build_service::util;
using namespace autil::legacy;
using namespace autil;

using namespace indexlib::partition;

namespace build_service { namespace task_base {

BS_LOG_SETUP(task_base, RollbackTask);

const string RollbackTask::TASK_NAME = BS_ROLLBACK_INDEX;

RollbackTask::RollbackTask() : _isFinished(false), _sourceVersion(-1), _targetVersion(-1), _partitionCount(0) {}

bool RollbackTask::init(TaskInitParam& initParam)
{
    _taskInitParam = initParam;
    if (initParam.clusterName.empty()) {
        string errorMsg = "partition id has no cluster name";
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return false;
    }
    return true;
}

bool RollbackTask::isDone(const config::TaskTarget& target) { return _isFinished; }

indexlib::util::CounterMapPtr RollbackTask::getCounterMap() { return indexlib::util::CounterMapPtr(); }

string RollbackTask::getTaskStatus() { return ToJsonString(_kvMap); }

bool RollbackTask::handleTarget(const config::TaskTarget& target)
{
    if (_isFinished) {
        return true;
    }
    if (!prepareParams(target)) {
        string errorMsg = "task params is invalid";
        REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
        return false;
    }

    vector<Range> ranges = RangeUtil::splitRange(RANGE_MIN, RANGE_MAX, _partitionCount);

    for (const auto& partRange : ranges) {
        PartitionId pid;
        *pid.mutable_range() = partRange;

        *pid.mutable_buildid() = _buildId;
        *pid.add_clusternames() = _taskInitParam.clusterName;

        string partIndexRoot = IndexPathConstructor::constructIndexPath(_indexRoot, pid);
        if (!IndexRollBackUtil::CreateRollBackVersion(partIndexRoot, _sourceVersion, _targetVersion,
                                                      _taskInitParam.epochId)) {
            BS_LOG(ERROR, "create roll back from version[%d] to version[%d] failed", _sourceVersion, _targetVersion);
            string errorMsg = "create rollback version failed.";
            REPORT_ERROR_WITH_ADVICE(SERVICE_ERROR_CONFIG, errorMsg, BS_STOP);
            return false;
        }
        BS_LOG(INFO, "create roll back version success, sourceVersion[%d], targetVersion[%d]", _sourceVersion,
               _targetVersion);
    }
    _isFinished = true;
    _kvMap[BS_ROLLBACK_TARGET_VERSION] = StringUtil::toString(_targetVersion);
    return true;
}

bool RollbackTask::prepareParams(const config::TaskTarget& target)
{
    string sourceVersionIdStr;
    if (!target.getTargetDescription(BS_ROLLBACK_SOURCE_VERSION, sourceVersionIdStr)) {
        BS_LOG(ERROR, "lack of sourceVersion");
        return false;
    }
    if (!StringUtil::fromString(sourceVersionIdStr, _sourceVersion)) {
        BS_LOG(ERROR, "sourceVersion[%d] is invalid.", _sourceVersion);
        return false;
    }

    auto resourceReader = _taskInitParam.resourceReader;
    BuildRuleConfig buildRuleConfig;
    if (!resourceReader->getClusterConfigWithJsonPath(_taskInitParam.clusterName, "cluster_config.builder_rule_config",
                                                      buildRuleConfig)) {
        string errorMsg = "parse cluster_config.builder_rule_config for [" + _taskInitParam.clusterName + "] failed";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    _partitionCount = buildRuleConfig.partitionCount;

    BuildServiceConfig buildServiceConfig;
    if (!resourceReader->getConfigWithJsonPath("build_app.json", "", buildServiceConfig)) {
        BS_LOG(ERROR, "failed to get build_app.json in config[%s]", resourceReader->getConfigPath().c_str());
        return false;
    }
    _indexRoot = buildServiceConfig.getIndexRoot();
    if (_indexRoot.empty()) {
        BS_LOG(ERROR, "index root is empty.");
        return false;
    }

    _buildId.set_datatable(_taskInitParam.buildId.dataTable);
    _buildId.set_generationid(_taskInitParam.buildId.generationId);
    _buildId.set_appname(_taskInitParam.buildId.appName);

    if (!prepareTargetVersion()) {
        return false;
    }
    return true;
}

bool RollbackTask::prepareTargetVersion()
{
    vector<Range> ranges = RangeUtil::splitRange(RANGE_MIN, RANGE_MAX, _partitionCount);

    versionid_t maxVersionId = indexlib::INVALID_VERSIONID;
    for (const auto& partRange : ranges) {
        PartitionId pid;
        *pid.mutable_range() = partRange;
        *pid.mutable_buildid() = _buildId;
        *pid.add_clusternames() = _taskInitParam.clusterName;

        string partIndexRoot = IndexPathConstructor::constructIndexPath(_indexRoot, pid);
        versionid_t versionId = IndexRollBackUtil::GetLatestOnDiskVersion(partIndexRoot);

        if (versionId == indexlib::INVALID_VERSIONID) {
            BS_LOG(ERROR, "get version from [%s] failed", partIndexRoot.c_str());
            return false;
        }
        if (versionId > maxVersionId) {
            maxVersionId = versionId;
        }
    }
    _targetVersion = maxVersionId + 1;
    return true;
}

}} // namespace build_service::task_base
