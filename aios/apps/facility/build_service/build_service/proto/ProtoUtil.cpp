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
#include "build_service/proto/ProtoUtil.h"

#include "autil/StringUtil.h"
#include "build_service/proto/ProcessorTaskIdentifier.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace build_service { namespace proto {

BS_LOG_SETUP(proto, ProtoUtil);

// merger.inc2.data_table.1.0.65535.mainse_searcher

string ProtoUtil::ROLE_NAME_BUILDER = "builder";
string ProtoUtil::ROLE_NAME_PROCESSOR = "processor";
string ProtoUtil::ROLE_NAME_MERGER = "merger";
string ProtoUtil::ROLE_NAME_TASK = "task";
string ProtoUtil::ROLE_NAME_JOB = "job";
string ProtoUtil::ROLE_NAME_AGENT = "agent";
string ProtoUtil::ROLE_NAME_UNKNOWN = "unknown";

bool ProtoUtil::partitionIdToRoleGroupId(const PartitionId& pid, string& roleGroupId)
{
    string roleId;
    if (!partitionIdToRoleId(pid, roleId)) {
        return false;
    }
    return roleIdToRoleGroupId(roleId, roleGroupId);
}

std::string ProtoUtil::getGeneralTaskAppName(const std::string& appName, const std::string& clusterName,
                                             uint16_t rangeFrom, int16_t rangeTo)
{
    return appName + "#" + clusterName + "_" + std::to_string(rangeFrom) + "_" + std::to_string(rangeTo);
}

std::string ProtoUtil::getOriginalAppName(const std::string& appName)
{
    std::vector<std::string> splitAppNames;
    autil::StringUtil::fromString(appName, splitAppNames, "#");
    if (splitAppNames.size() == 2) {
        return splitAppNames[0];
    }
    return std::string();
}

bool ProtoUtil::partitionIdToRoleId(const PartitionId& pid, string& roleId, bool ignoreBackupId)
{
    if (pid.role() == ROLE_UNKNOWN) {
        return false;
    }
    vector<string> strVec;
    strVec.push_back(StringUtil::toString(pid.buildid().appname()));
    strVec.push_back(StringUtil::toString(pid.buildid().datatable()));
    strVec.push_back(StringUtil::toString(pid.buildid().generationid()));
    strVec.push_back(toRoleString(pid.role()));

    switch (pid.role()) {
    case ROLE_PROCESSOR:
    case ROLE_BUILDER:
    case ROLE_JOB: {
        strVec.push_back(toStepString(pid));
        break;
    }
    case ROLE_MERGER: {
        strVec.push_back(pid.mergeconfigname());
        break;
    }
    case ROLE_AGENT:
    case ROLE_TASK: {
        strVec.push_back(pid.taskid());
        break;
    }
    default:
        break;
    }

    strVec.push_back(StringUtil::toString(pid.range().from()));
    strVec.push_back(StringUtil::toString(pid.range().to()));

    if (pid.role() == ROLE_BUILDER || pid.role() == ROLE_MERGER || pid.role() == ROLE_JOB || pid.role() == ROLE_TASK ||
        pid.role() == ROLE_AGENT) {
        if (pid.clusternames_size() != 1) {
            return false;
        }
        strVec.push_back(pid.clusternames(0));
    }
    if (pid.role() == ROLE_PROCESSOR || pid.role() == ROLE_JOB) {
        strVec.push_back(pid.taskid());
    }
    if (pid.role() == ROLE_MERGER && pid.taskid() != "") {
        strVec.push_back(pid.taskid());
    }

    // note that backup node label must be at the second last postition,
    // backup id must be at the last posititon.
    if (pid.has_backupid() && !ignoreBackupId) {
        strVec.push_back(BS_BACKUP_NODE_LABEL);
        strVec.push_back(StringUtil::toString(pid.backupid()));
    }
    roleId = StringUtil::toString(strVec, ".");
    return true;
}

bool ProtoUtil::roleIdToRoleGroupId(const string& roleId, string& roleGroupId)
{
    vector<string> strVec = StringUtil::split(roleId, ".", false);
    size_t totalSize = strVec.size();
    if (totalSize > 2 && strVec[totalSize - 2] == BS_BACKUP_NODE_LABEL) {
        roleGroupId =
            roleId.substr(0, roleId.length() - strVec[totalSize - 2].length() - strVec[totalSize - 1].length() - 2);
    } else {
        roleGroupId = roleId;
    }
    return true;
}

bool ProtoUtil::roleIdToPartitionId(const string& roleId, const string& appName, PartitionId& pid)
{
    if (roleId.empty()) {
        return false;
    }

    size_t pos = roleId.find(appName);
    if (pos != 0) {
        return false;
    }

    if (roleId.length() <= appName.length()) {
        return false;
    }

    string roleStr = roleId.substr(appName.length() + 1); // skip first '.'
    vector<string> strVec = StringUtil::split(roleStr, ".", false);

    if (strVec.size() < 7) {
        return false;
    }

    if (strVec[strVec.size() - 2] == BS_BACKUP_NODE_LABEL) {
        uint32_t backupId;
        if (!StringUtil::fromString(strVec[strVec.size() - 1], backupId)) {
            return false;
        }
        pid.set_backupid(backupId);
        strVec.pop_back();
        strVec.pop_back();
    }

    pid.mutable_buildid()->set_appname(appName);
    pid.mutable_buildid()->set_datatable(strVec[0]);
    uint32_t gid;
    if (!StringUtil::fromString(strVec[1], gid)) {
        return false;
    }
    pid.mutable_buildid()->set_generationid(gid);
    RoleType role = fromRoleString(strVec[2]);
    if (role == ROLE_UNKNOWN) {
        return false;
    }
    pid.set_role(role);
    if (role == ROLE_MERGER) {
        pid.set_mergeconfigname(strVec[3]);

    } else if (role == ROLE_TASK || role == ROLE_AGENT) {
        pid.set_taskid(strVec[3]);
    } else {
        pid.set_step(strVec[3] == "full" ? BUILD_STEP_FULL : BUILD_STEP_INC);
    }

    uint16_t from, to;
    if (!StringUtil::fromString(strVec[4], from) || !StringUtil::fromString(strVec[5], to)) {
        return false;
    }
    pid.mutable_range()->set_from(from);
    pid.mutable_range()->set_to(to);
    if (role == ROLE_BUILDER || role == ROLE_MERGER || role == ROLE_TASK || role == ROLE_AGENT) {
        *pid.add_clusternames() = strVec[6];
    }
    if (role == ROLE_PROCESSOR) {
        *pid.mutable_taskid() = strVec[6];
    }
    if (role == ROLE_JOB) {
        *pid.add_clusternames() = strVec[6];
        if (strVec.size() < 8) {
            return false;
        }
        *pid.mutable_taskid() = strVec[7];
    }
    if (role == ROLE_MERGER && strVec.size() == 8) {
        *pid.mutable_taskid() = strVec[7];
    }
    return true;
}

bool ProtoUtil::partitionIdToCounterFileName(const PartitionId& pid, string& counterFileName, bool ignoreBackupId)
{
    if (pid.role() == ROLE_UNKNOWN) {
        return false;
    }
    vector<string> strVec;
    strVec.push_back(StringUtil::toString(pid.buildid().appname()));
    strVec.push_back(StringUtil::toString(pid.buildid().datatable()));
    strVec.push_back(StringUtil::toString(pid.buildid().generationid()));
    TaskIdentifier taskIdentifier;
    if (pid.role() == ROLE_TASK) {
        if (!taskIdentifier.fromString(pid.taskid())) {
            return false;
        }
    }
    std::string taskName;
    if (taskIdentifier.getTaskName(taskName) && taskName == "builderV2") {
        strVec.push_back(toRoleString(ROLE_BUILDER));
        if (taskIdentifier.getTaskId() == "fullBuilder") {
            strVec.push_back("full");
        } else {
            strVec.push_back("inc");
        }
    } else {
        strVec.push_back(toRoleString(pid.role()));

        switch (pid.role()) {
        case ROLE_PROCESSOR:
        case ROLE_BUILDER:
        case ROLE_JOB: {
            strVec.push_back(toStepString(pid));
            break;
        }
        case ROLE_MERGER:
        case ROLE_TASK: {
            strVec.push_back(pid.mergeconfigname());
            break;
        }
        default:
            break;
        }
    }

    strVec.push_back(StringUtil::toString(pid.range().from()));
    strVec.push_back(StringUtil::toString(pid.range().to()));

    if (pid.role() == ROLE_BUILDER || pid.role() == ROLE_MERGER || pid.role() == ROLE_JOB || pid.role() == ROLE_TASK) {
        if (pid.clusternames_size() != 1) {
            return false;
        }
        strVec.push_back(pid.clusternames(0));
    }
    // inc processor counter will inherit from inc processor counter
    // when schema is changed, inc processor counter will be redirected to new counter file
    ProcessorTaskIdentifier processorIdentifier;
    if (pid.role() == ROLE_PROCESSOR && processorIdentifier.fromString(pid.taskid())) {
        string clusterName;
        processorIdentifier.getClusterName(clusterName);
        if (!clusterName.empty()) {
            strVec.push_back(clusterName);
        }

        string processorTaskId = processorIdentifier.getTaskId();
        if (processorTaskId != "0" && !processorTaskId.empty()) {
            strVec.push_back(processorTaskId);
        }
    }
    if (pid.has_backupid() && !ignoreBackupId) {
        strVec.push_back(BS_BACKUP_NODE_LABEL);
        strVec.push_back(StringUtil::toString(pid.backupid()));
    }
    counterFileName = StringUtil::toString(strVec, ".");
    return true;
}

uint32_t ProtoUtil::getProcessorSchemaUpdateCount(const string& processorTaskId)
{
    vector<uint32_t> nums;
    StringUtil::fromString(processorTaskId, nums, "-");
    if (nums.size() <= 1) {
        return 0;
    }

    if (nums.size() == 2) {
        return nums[1];
    }
    assert(false);
    return 0;
}
string ProtoUtil::adminToRoleId(uint32_t generationId, const string& datatable, const string& clusterName)
{
    stringstream ss;
    ss << "admin." << datatable << "." << StringUtil::toString(generationId) << "." << clusterName;
    return ss.str();
}

string ProtoUtil::getRawDocTopicName(const string& prefix, const BuildId& buildId)
{
    return prefix + "_raw_" + buildId.datatable() + "_" + StringUtil::toString(buildId.generationid());
}

string ProtoUtil::getProcessedDocTopicName(const string& prefix, const BuildId& buildId, const string& clusterName)
{
    return prefix + "_processed_" + StringUtil::toString(buildId.generationid()) + "_" + clusterName;
}

string& ProtoUtil::toRoleString(RoleType role)
{
    if (role == ROLE_BUILDER) {
        return ROLE_NAME_BUILDER;
    } else if (role == ROLE_PROCESSOR) {
        return ROLE_NAME_PROCESSOR;
    } else if (role == ROLE_MERGER) {
        return ROLE_NAME_MERGER;
    } else if (role == ROLE_JOB) {
        return ROLE_NAME_JOB;
    } else if (role == ROLE_TASK) {
        return ROLE_NAME_TASK;
    } else if (role == ROLE_AGENT) {
        return ROLE_NAME_AGENT;
    } else {
        return ROLE_NAME_UNKNOWN;
    }
}

RoleType ProtoUtil::fromRoleString(const string& roleStr)
{
    if (roleStr == ROLE_NAME_BUILDER) {
        return ROLE_BUILDER;
    } else if (roleStr == ROLE_NAME_PROCESSOR) {
        return ROLE_PROCESSOR;
    } else if (roleStr == ROLE_NAME_MERGER) {
        return ROLE_MERGER;
    } else if (roleStr == ROLE_NAME_JOB) {
        return ROLE_JOB;
    } else if (roleStr == ROLE_NAME_TASK) {
        return ROLE_TASK;
    } else if (roleStr == ROLE_NAME_AGENT) {
        return ROLE_AGENT;
    } else {
        return ROLE_UNKNOWN;
    }
}

string ProtoUtil::toStepString(const PartitionId& pid)
{
    BuildStep step = pid.step();
    return step == BUILD_STEP_FULL ? "full" : "inc";
}

string ProtoUtil::toStepString(const proto::BuildStep& buildStep)
{
    return buildStep == BUILD_STEP_FULL ? "full" : "inc";
}

vector<string> ProtoUtil::getClusterNames(const proto::PartitionId& partitionId)
{
    vector<string> clusterNames;
    for (int32_t i = 0; i < partitionId.clusternames_size(); ++i) {
        clusterNames.push_back(partitionId.clusternames(i));
    }
    return clusterNames;
}

bool ProtoUtil::isEqual(const BuildId& left, const BuildId& right)
{
    // check appname
    if (!left.has_appname() && right.has_appname()) {
        return false;
    }
    if (left.has_appname() && !right.has_appname()) {
        return false;
    }
    if (left.has_appname() && right.has_appname() && left.appname() != right.appname()) {
        return false;
    }
    // check datatable
    if (!left.has_datatable() && right.has_datatable()) {
        return false;
    }
    if (left.has_datatable() && !right.has_datatable()) {
        return false;
    }
    if (left.has_datatable() && right.has_datatable() && left.datatable() != right.datatable()) {
        return false;
    }
    // check generationid
    if (!left.has_generationid() && right.has_generationid()) {
        return false;
    }
    if (left.has_generationid() && !right.has_generationid()) {
        return false;
    }
    if (left.has_generationid() && right.has_generationid() && left.generationid() != right.generationid()) {
        return false;
    }
    return true;
}

void BuildIdWrapper::Jsonize(JsonWrapper& json)
{
    if (json.GetMode() == TO_JSON) {
#define BUILD_ID_PARAM_TO_JSON(TYPE, NAME)                                                                             \
    if (buildId.has_##NAME()) {                                                                                        \
        TYPE NAME = buildId.NAME();                                                                                    \
        json.Jsonize(#NAME, NAME);                                                                                     \
    }

        BUILD_ID_PARAM_TO_JSON(string, datatable);
        BUILD_ID_PARAM_TO_JSON(uint32_t, generationid);
        BUILD_ID_PARAM_TO_JSON(string, appname);
#undef BUILD_ID_PARAM_TO_JSON

    } else {
        JsonMap map = json.GetMap();

#define BUILD_ID_PARAM_FROM_JSON(TYPE, NAME)                                                                           \
    if (map.find(#NAME) != map.end()) {                                                                                \
        TYPE NAME;                                                                                                     \
        json.Jsonize(#NAME, NAME);                                                                                     \
        buildId.set_##NAME(NAME);                                                                                      \
    }

        BUILD_ID_PARAM_FROM_JSON(string, datatable);
        BUILD_ID_PARAM_FROM_JSON(uint32_t, generationid);
        BUILD_ID_PARAM_FROM_JSON(string, appname);
#undef BUILD_ID_PARAM_FROM_JSON
    }
}

bool ProtoUtil::strToBuildId(const string& str, proto::BuildId& buildId)
{
    Any any;
    ParseJson(str, any);
    Jsonizable::JsonWrapper json(any);
    BuildIdWrapper wrapper;
    try {
        wrapper.Jsonize(json);
    } catch (ExceptionBase& e) {
        BS_LOG(ERROR, "buildId [%s] from json fail, reason [%s]", str.c_str(), e.what());
        return false;
    }
    buildId = wrapper.buildId;
    return true;
}

string ProtoUtil::buildIdToStr(const proto::BuildId& buildId)
{
    BuildIdWrapper wrapper;
    wrapper.buildId = buildId;
    return ToJsonString(wrapper, true);
}

void ProtoUtil::partitionIdToBeeperTags(const proto::PartitionId& pid, beeper::EventTags& tags)
{
    buildIdToBeeperTags(pid.buildid(), tags);
    if (pid.has_mergeconfigname()) {
        tags.AddTag("mergeConfigName", pid.mergeconfigname());
    }

    if (pid.has_taskid()) {
        tags.AddTag("taskId", pid.taskid());
    }

    tags.AddTag("range", StringUtil::toString(pid.range().from()) + "_" + StringUtil::toString(pid.range().to()));
    if (pid.clusternames().size() > 0) {
        tags.AddTag("clusters", StringUtil::toString(ProtoUtil::getClusterNames(pid), " "));
    }
    tags.AddTag("buildStep", ProtoUtil::toStepString(pid));
    tags.AddTag("roleType", ProtoUtil::toRoleString(pid.role()));
    if (pid.has_backupid()) {
        tags.AddTag("backupId", StringUtil::toString(pid.backupid()));
    }
}

void ProtoUtil::buildIdToBeeperTags(const proto::BuildId& buildId, beeper::EventTags& tags)
{
    tags.AddTag("generation", StringUtil::toString(buildId.generationid()));
    if (!buildId.appname().empty()) {
        tags.AddTag("buildAppName", buildId.appname());
    }
    if (!buildId.datatable().empty()) {
        tags.AddTag("dataTable", buildId.datatable());
    }
}

const char* ProtoUtil::getServiceErrorCodeName(ServiceErrorCode errorCode)
{
    switch (errorCode) {
    case SERVICE_ERROR_NONE:
        return "SERVICE_ERROR_NONE";
    case SERVICE_ERROR_CONFIG:
        return "SERVICE_ERROR_CONFIG";
    case READER_ERROR_INIT_METRICS:
        return "READER_ERROR_INIT_METRICS";
    case READER_ERROR_READ_EXCEPTION:
        return "READER_ERROR_READ_EXCEPTION";
    case READER_ERROR_CONNECT_SWIFT:
        return "READER_ERROR_CONNECT_SWIFT";
    case READER_ERROR_SWIFT_CREATE_READER:
        return "READER_ERROR_SWIFT_CREATE_READER";
    case READER_ERROR_SWIFT_SEEK:
        return "READER_ERROR_SWIFT_SEEK";
    case PROCESSOR_ERROR_INIT_METRICS:
        return "PROCESSOR_ERROR_INIT_METRICS";
    case BUILDER_ERROR_BUILD_FILEIO:
        return "BUILDER_ERROR_BUILD_FILEIO";
    case BUILDER_ERROR_MERGE_FILEIO:
        return "BUILDER_ERROR_MERGE_FILEIO";
    case BUILDER_ERROR_DUMP_FILEIO:
        return "BUILDER_ERROR_DUMP_FILEIO";
    case BUILDER_ERROR_REACH_MAX_RESOURCE:
        return "BUILDER_ERROR_REACH_MAX_RESOURCE";
    case BUILDER_ERROR_GET_INC_LOCATOR:
        return "BUILDER_ERROR_GET_INC_LOCATOR";
    case BUILDER_ERROR_GET_LAST_LOCATOR:
        return "BUILDER_ERROR_GET_LAST_LOCATOR";
    case BUILDER_ERROR_UNKNOWN:
        return "BUILDER_ERROR_UNKNOWN";
    case BUILDFLOW_ERROR_CREATE_READER:
        return "BUILDFLOW_ERROR_CREATE_READER";
    case BUILDFLOW_ERROR_CREATE_PROCESSOR:
        return "BUILDFLOW_ERROR_CREATE_PROCESSOR";
    case BUILDFLOW_ERROR_CREATE_DOC_HANDLER:
        return "BUILDFLOW_ERROR_CREATE_DOC_HANDLER";
    case BUILDFLOW_ERROR_CREATE_SRC_NODE:
        return "BUILDFLOW_ERROR_CREATE_SRC_NODE";
    case BUILDFLOW_ERROR_CREATE_BUILDER:
        return "BUILDFLOW_ERROR_CREATE_BUILDER";
    case BUILDFLOW_ERROR_SEEK:
        return "BUILDFLOW_ERROR_SEEK";
    case BUILDFLOW_ERROR_CONNECT_SWIFT:
        return "BUILDFLOW_ERROR_CONNECT_SWIFT";
    case BUILDFLOW_ERROR_CLUSTER:
        return "BUILDFLOW_ERROR_CLUSTER";
    case BUILDFLOW_ERROR_SWIFT_CREATE_READER:
        return "BUILDFLOW_ERROR_SWIFT_CREATE_READER";
    case BUILDFLOW_ERROR_SWIFT_CREATE_WRITER:
        return "BUILDFLOW_ERROR_SWIFT_CREATE_WRITER";
    case BUILDFLOW_ERROR_INIT_SWIFT_PRODUCER:
        return "BUILDFLOW_ERROR_INIT_SWIFT_PRODUCER";
    case BUILDFLOW_ERROR_CHECKPOINT_PATH:
        return "BUILDFLOW_ERROR_CHECKPOINT_PATH";
    case BUILDFLOW_ERROR_SWIFT_SEEK:
        return "BUILDFLOW_ERROR_SWIFT_SEEK";
    case MERGER_ERROR_FAILED:
        return "MERGER_ERROR_FAILED";
    case SERVICE_ERROR_DOWNLOAD_CONFIG:
        return "SERVICE_ERROR_DOWNLOAD_CONFIG";
    case SERVICE_ADMIN_ERROR:
        return "SERVICE_ADMIN_ERROR";
    case SERVICE_TASK_ERROR:
        return "SERVICE_TASK_ERROR";
    default:
        return "unknown";
    }
}

const char* ProtoUtil::getErrorAdviceName(ErrorAdvice errorAdvice)
{
    switch (errorAdvice) {
    case BS_RETRY:
        return "BS_RETRY";
    case BS_STOP:
        return "BS_STOP";
    case BS_IGNORE:
        return "BS_IGNORE";
    case BS_FATAL_ERROR:
        return "BS_FATAL_ERROR";
    default:
        return "unknown";
    }
}

}} // namespace build_service::proto
