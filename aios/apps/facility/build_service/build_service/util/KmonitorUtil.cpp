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
#include "build_service/util/KmonitorUtil.h"

#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "build_service/proto/ProtoUtil.h"
#include "build_service/proto/TaskIdentifier.h"

using namespace std;
using namespace autil;
using namespace kmonitor;
using namespace build_service::proto;

namespace build_service { namespace util {

BS_LOG_SETUP(util, KmonitorUtil);

bool KmonitorUtil::validateAndAddTag(const std::string& tagk, const std::string& tagv, kmonitor::MetricsTags& tags)
{
    if (tagk.empty() || tagv.empty()) {
        BS_LOG(WARN, "invalid kmnonitor tag kv: tagk[%s], tagv[%s]", tagk.c_str(), tagv.c_str());
        return false;
    }
    tags.AddTag(tagk, tagv);
    return true;
}

void KmonitorUtil::getTags(const proto::PartitionId& pid, kmonitor::MetricsTags& tags)
{
    string generationId = StringUtil::toString(pid.buildid().generationid());
    validateAndAddTag("generation", generationId, tags);
    string roleName = ProtoUtil::toRoleString(pid.role());
    proto::TaskIdentifier taskIdentifier;
    taskIdentifier.fromString(pid.taskid());
    string taskName;
    bool ret = taskIdentifier.getTaskName(taskName);
    if (pid.role() == proto::ROLE_TASK && ret && taskName == "builderV2") {
        roleName = ProtoUtil::toRoleString(RoleType::ROLE_BUILDER);
        string clusterName = pid.clusternames(0);
        validateAndAddTag("cluster", clusterName, tags);
        string taskId = taskIdentifier.getTaskId();
        string step = taskId.find("full") == 0 ? "full" : "inc";
        validateAndAddTag("step", step, tags);
    }
    validateAndAddTag("role", roleName, tags);
    string partition = StringUtil::toString(pid.range().from());
    partition += "_" + StringUtil::toString(pid.range().to());
    validateAndAddTag("partition", partition, tags);

    if (pid.role() == RoleType::ROLE_PROCESSOR) {
        string step = ProtoUtil::toStepString(pid);
        validateAndAddTag("step", step, tags);
    }
    if (pid.role() == RoleType::ROLE_BUILDER) {
        string clusterName = pid.clusternames(0);
        validateAndAddTag("cluster", clusterName, tags);
        string step = ProtoUtil::toStepString(pid);
        validateAndAddTag("step", step, tags);
    }
    if (pid.role() == RoleType::ROLE_MERGER) {
        string clusterName = pid.clusternames(0);
        validateAndAddTag("cluster", clusterName, tags);
        validateAndAddTag("mergeConfig", pid.mergeconfigname(), tags);
    }
    string tableName = StringUtil::toString(pid.buildid().datatable());
    validateAndAddTag("table_name", tableName, tags);
    validateAndAddTag("app", getApp(), tags);
    validateAndAddTag("appName", StringUtil::toString(pid.buildid().appname()), tags);

    int32_t backupId = pid.has_backupid() ? pid.backupid() : 0;
    validateAndAddTag("backupId", std::to_string(backupId), tags);
}

void KmonitorUtil::getTags(const Range& range, const string& clusterName, const string& dataTable,
                           generationid_t generationId, kmonitor::MetricsTags& tags)
{
    validateAndAddTag("generation", StringUtil::toString(generationId), tags);
    string partition = StringUtil::toString(range.from());
    partition += "_" + StringUtil::toString(range.to());
    validateAndAddTag("partition", partition, tags);
    validateAndAddTag("cluster", clusterName, tags);
    validateAndAddTag("table_name", dataTable, tags);
    validateAndAddTag("app", getApp(), tags);
}

string KmonitorUtil::getApp()
{
    string app = "build_service";
    // for compatity
    EnvUtil::getEnvWithoutDefault("KMONITOR_ADAPTER_APP", app);
    // new env will override old env
    EnvUtil::getEnvWithoutDefault("build_service_app", app);
    return app;
}

}} // namespace build_service::util
