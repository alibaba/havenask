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
#include "build_service/admin/AdminTaskBase.h"

#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "beeper/beeper.h"
#include "build_service/config/ResourceReaderManager.h"

using namespace std;
using namespace build_service::config;
using namespace build_service::util;
using namespace beeper;

namespace build_service { namespace admin {

BS_LOG_SETUP(admin, AdminTaskBase);

int64_t AdminTaskBase::DEFAULT_BEEPER_REPORT_INTERVAL = 600;
AdminTaskBase::AdminTaskBase(const proto::BuildId& buildId, const TaskResourceManagerPtr& resMgr)
    : _buildId(buildId)
    , _slowNodeDetect(false)
    , _nodesStartTimestamp(-1)
    , _taskStatus(TASK_NORMAL)
    , _resourceManager(resMgr)
    , _nodeStatusManager(std::make_shared<taskcontroller::NodeStatusManager>(
          buildId,
          /*startTime=*/autil::TimeUtility::currentTimeInMicroSeconds()))
    , _hasCreateNodes(false)
    , _beeperTags(new beeper::EventTags)
    , _beeperReportTs(0)
    , _beeperReportInterval(
          autil::EnvUtil::getEnv(BS_ENV_ADMIN_TASK_BEEPER_REPORT_INTERVAL, DEFAULT_BEEPER_REPORT_INTERVAL))
{
    setBeeperCollector(GENERATION_ERROR_COLLECTOR_NAME);
    initBeeperTag(buildId);
}

AdminTaskBase::AdminTaskBase()
    : _slowNodeDetect(false)
    , _nodesStartTimestamp(-1)
    , _taskStatus(TASK_NORMAL)
    , _beeperReportTs(0)
    , _beeperReportInterval(
          autil::EnvUtil::getEnv(BS_ENV_ADMIN_TASK_BEEPER_REPORT_INTERVAL, DEFAULT_BEEPER_REPORT_INTERVAL))
{
    setBeeperCollector(GENERATION_ERROR_COLLECTOR_NAME);
}

void AdminTaskBase::setBeeperTags(const EventTagsPtr beeperTags) { _beeperTags = beeperTags; }

bool AdminTaskBase::getSlowNodeDetectConfig(const ResourceReaderPtr& resourceReader, const std::string& clusterName,
                                            SlowNodeDetectConfig& slowNodeDetectConfig)
{
    if (!clusterName.empty()) {
        config::SlowNodeDetectConfig clusterSlowDetectConfig;
        bool configExist = false;
        if (resourceReader->getClusterConfigWithJsonPath(clusterName, "slow_node_detect_config",
                                                         clusterSlowDetectConfig, configExist)) {
            if (configExist) {
                BS_LOG(INFO, "cluster[%s] use customized slow_node_detect_config", clusterName.c_str());
                slowNodeDetectConfig = clusterSlowDetectConfig;
                return true;
            }
        }
    }
    config::SlowNodeDetectConfig defaultSlowDetectConfig;
    if (!resourceReader->getDataTableConfigWithJsonPath(_buildId.datatable(), "slow_node_detect_config",
                                                        defaultSlowDetectConfig)) {
        BS_LOG(ERROR, "slow_node_detect_config is invalid");
        return false;
    }
    slowNodeDetectConfig = defaultSlowDetectConfig;
    return true;
}

bool AdminTaskBase::initSlowNodeDetect(const ResourceReaderPtr& resourceReader, const string& clusterName)
{
    config::SlowNodeDetectConfig slowNodeDetectConfig;
    if (!getSlowNodeDetectConfig(resourceReader, clusterName, slowNodeDetectConfig)) {
        BS_LOG(ERROR, "get slowNodeDetectConfig from %s failed", resourceReader->getOriginalConfigPath().c_str());
        BEEPER_FORMAT_REPORT(GENERATION_STATUS_COLLECTOR_NAME, *_beeperTags, "get slowNodeDetectConfig from %s failed",
                             resourceReader->getOriginalConfigPath().c_str());
        return false;
    }
    _slowNodeDetect = slowNodeDetectConfig.enableSlowDetect;
    if (_slowNodeDetect) {
        BS_LOG(INFO, "init slowNodeDetectConfig[%s] for %s", ToJsonString(slowNodeDetectConfig, true).c_str(),
               clusterName.c_str());
        prepareSlowNodeDetector(clusterName);
    }
    return true;
}

void AdminTaskBase::doClearFullWorkerZkNode(const std::string& generationDir, const proto::RoleType roleType,
                                            const std::string& clusterName) const
{
    std::vector<std::string> fileList;
    fslib::util::FileUtil::listDir(generationDir, fileList);
    for (const auto& fileName : fileList) {
        proto::PartitionId pid;
        proto::ProtoUtil::roleIdToPartitionId(fileName, _buildId.appname(), pid);
        if (!pid.has_role() || pid.role() != roleType) {
            continue;
        }

        if (pid.role() == proto::ROLE_MERGER) {
            if (!pid.has_mergeconfigname() || pid.mergeconfigname() != "full") {
                continue;
            }
        } else if (!pid.has_step() || pid.step() != proto::BUILD_STEP_FULL) {
            continue;
        }

        if (!pid.has_buildid() || pid.buildid() != _buildId) {
            continue;
        }

        if (roleType != proto::ROLE_PROCESSOR &&
            (pid.clusternames_size() != 1u || pid.clusternames(0) != clusterName)) {
            continue;
        }

        std::string rolePath = fslib::util::FileUtil::joinFilePath(generationDir, fileName);
        if (!fslib::util::FileUtil::remove(rolePath)) {
            BS_LOG(WARN, "remove full worker %s failed", rolePath.c_str());
        }
    }
}

string AdminTaskBase::getDateFormatString(int64_t inputTs)
{
    string ret;
    if (autil::StringUtil::tryConvertToDateInMonth(inputTs, ret) ||
        autil::StringUtil::tryConvertToDateInMonth(inputTs / 1000000, ret)) {
        return ret;
    }
    ret = autil::StringUtil::toString(inputTs);
    return ret;
}

void AdminTaskBase::intervalBeeperReport(const string& beeperId, string format, ...)
{
    int64_t now = autil::TimeUtility::currentTimeInSeconds();
    if (now - _beeperReportTs <= _beeperReportInterval) {
        return;
    }
    va_list args;
    va_start(args, format);
    char buffer[1024];
    vsnprintf(buffer, 1024, format.c_str(), args);
    BS_LOG(INFO, "%s", buffer);
    va_end(args);
    BEEPER_REPORT(beeperId, buffer, *_beeperTags);
    _beeperReportTs = now;
    if (_slowNodeDetector) {
        _reportedSlowNodeMetric = _slowNodeDetector->getMetric();
    } else {
        BS_LOG(WARN, "try to report slow node metric, but _slowNodeDetector is nullptr");
    }
}

void AdminTaskBase::ReportSlowNodeMetrics(const std::string& clusterName, const SlowNodeHandleStrategy::Metric& metric)
{
    SlowNodeMetricReporterPtr reporter;
    _resourceManager->getResource(reporter);
    if (_nodeStatusManager->GetAllNodes().empty()) {
        return;
    }
    auto firstNodeStatus = _nodeStatusManager->GetAllNodes()[0];
    if (reporter) {
        SlowNodeMetricReporter::MetricRecord record;
        record.slowNodeMigrateTimes = metric.slowNodeKillTimes;
        record.deadNodeMigrateTimes = metric.deadNodeKillTimes;
        record.restartNodeMigrateTimes = metric.restartNodeKillTimes;
        record.backupNodeCreateTimes = metric.slowNodeBackupCreateTimes;
        record.effectiveBackupNodeCount = metric.effectiveBackupNodeCount;
        std::string roleStr = proto::ProtoUtil::toRoleString(static_cast<proto::RoleType>(firstNodeStatus->roleType));
        std::string stepStr = proto::ProtoUtil::toStepString(static_cast<proto::BuildStep>(firstNodeStatus->buildStep));
        if (firstNodeStatus->mergeConfigName != "") {
            // merger/task buildStep is not set, use mergeConfigName instead.
            stepStr = firstNodeStatus->mergeConfigName;
        }
        kmonitor::MetricsTags kmonTags;
        kmonTags.AddTag("role_type", roleStr);
        kmonTags.AddTag("build_step", stepStr);
        kmonTags.AddTag("cluster", clusterName);
        reporter->ReportMetrics(record, kmonTags);
    }
}

}} // namespace build_service::admin
