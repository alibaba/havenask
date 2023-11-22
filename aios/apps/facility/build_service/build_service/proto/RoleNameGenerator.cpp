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
#include "build_service/proto/RoleNameGenerator.h"

#include <assert.h>
#include <cstddef>
#include <math.h>
#include <stdint.h>

#include "autil/RangeUtil.h"
#include "autil/Span.h"
#include "autil/StringUtil.h"
#include "build_service/proto/ProtoUtil.h"
#include "build_service/proto/TaskIdentifier.h"

using namespace std;
using namespace autil;

namespace build_service { namespace proto {

BS_LOG_SETUP(admin, RoleNameGenerator);

RoleNameGenerator::RoleNameGenerator() {}

RoleNameGenerator::~RoleNameGenerator() {}

std::vector<GroupRolePair> RoleNameGenerator::generateGroupRolePairs(const std::vector<PartitionId>& partitionIds)
{
    std::vector<GroupRolePair> vec;
    vec.reserve(partitionIds.size());
    for (size_t i = 0; i < partitionIds.size(); ++i) {
        vec.push_back(generateGroupRolePair(partitionIds[i]));
    }
    return vec;
}

GroupRolePair RoleNameGenerator::generateGroupRolePair(const PartitionId& partitionId)
{
    return make_pair(generateGroupName(partitionId), generateRoleName(partitionId));
}

void RoleNameGenerator::fillTaskItems(const PartitionId& partitionId, vector<string>& items)
{
    TaskIdentifier taskIdentifier;
    taskIdentifier.fromString(partitionId.taskid());
    string taskName;
    bool ret = taskIdentifier.getTaskName(taskName);
    if (ret && taskName == "builderV2") {
        items.push_back(ProtoUtil::toRoleString(ROLE_BUILDER));
        items.push_back(partitionId.clusternames(0));
        auto taskId = taskIdentifier.getTaskId();
        if (taskId == "fullBuilder") {
            items.push_back("full");
        } else {
            items.push_back("inc");
        }
        return;
    }
    string name;
    ret = taskIdentifier.getValue("name", name);
    if (ret && name == "fullMerge") {
        items.push_back(ProtoUtil::toRoleString(ROLE_MERGER));
        items.push_back(partitionId.clusternames(0));
        items.push_back("full");
        return;
    }
    if (ret && name == "incMerge") {
        items.push_back(ProtoUtil::toRoleString(ROLE_MERGER));
        items.push_back(partitionId.clusternames(0));
        items.push_back("inc");
        return;
    }

    items.push_back(ProtoUtil::toRoleString(ROLE_TASK));
    items.push_back(partitionId.clusternames(0));
    items.push_back(partitionId.taskid());
}

string RoleNameGenerator::generateGroupName(const PartitionId& partitionId)
{
    RoleType role = partitionId.role();
    vector<string> items;
    items.reserve(5);

    switch (role) {
    case ROLE_BUILDER:
        items.push_back(ProtoUtil::toRoleString(role));
        assert(1 == partitionId.clusternames_size());
        items.push_back(partitionId.clusternames(0));
        items.push_back(ProtoUtil::toStepString(partitionId));
        break;
    case ROLE_MERGER:
        items.push_back(ProtoUtil::toRoleString(role));
        assert(1 == partitionId.clusternames_size());
        items.push_back(partitionId.clusternames(0));
        items.push_back(partitionId.mergeconfigname());
        break;
    case ROLE_PROCESSOR:
        items.push_back(ProtoUtil::toRoleString(role));
        items.push_back(partitionId.buildid().datatable());
        items.push_back(ProtoUtil::toStepString(partitionId));
        items.push_back(partitionId.taskid());
        break;
    case ROLE_JOB:
        items.push_back(ProtoUtil::toRoleString(role));
        assert(1 == partitionId.clusternames_size());
        items.push_back(partitionId.buildid().datatable());
        items.push_back(partitionId.clusternames(0));
        items.push_back(ProtoUtil::toStepString(partitionId));
        break;
    case ROLE_TASK:
        assert(1 == partitionId.clusternames_size());
        fillTaskItems(partitionId, items);
        break;
    case ROLE_AGENT:
        items.push_back(ProtoUtil::toRoleString(role));
        items.push_back(partitionId.taskid());
        items.push_back(partitionId.clusternames(0));
        break;
    default:
        assert(false);
    }
    return StringUtil::toString(items, ".");
}

string RoleNameGenerator::generateRoleName(const PartitionId& partitionId)
{
    if (partitionId.role() != ROLE_PROCESSOR && partitionId.role() != ROLE_AGENT) {
        assert(0 != partitionId.clusternames().size());
    }
    string roleName;
    ProtoUtil::partitionIdToRoleId(partitionId, roleName);
    return roleName;
}

string RoleNameGenerator::generateRoleGroupName(const PartitionId& partitionId)
{
    string roleName = generateRoleName(partitionId);
    string roleGroupName;
    ProtoUtil::roleIdToRoleGroupId(roleName, roleGroupName);
    return roleGroupName;
}

string RoleNameGenerator::generateGangIdentifier(const PartitionId& partitionId)
{
    RoleType role = partitionId.role();
    BuildStep buildStep = partitionId.step();
    uint32_t generationId = partitionId.buildid().generationid();

    vector<string> items;
    items.reserve(3);
    items.push_back(StringUtil::toString(generationId));

    switch (role) {
    case ROLE_BUILDER:
    case ROLE_PROCESSOR:
    case ROLE_JOB: {
        if (buildStep == BUILD_STEP_FULL) {
            items.push_back(ProtoUtil::toStepString(partitionId));
        } else {
            items.clear();
        }
        break;
    }
    case ROLE_MERGER:
    case ROLE_ALTER_FIELD: {
        items.push_back(partitionId.clusternames(0));
        items.push_back(partitionId.mergeconfigname());
        break;
    }
    case ROLE_TASK:
        items.clear();
        break;
    default:
        assert(false);
    }
    return StringUtil::toString(items, "-");
}

void RoleNameGenerator::generateAgentNodePartitionId(const proto::BuildId& id, const std::string& appName,
                                                     const std::string& configName, size_t totalCnt, size_t idx,
                                                     proto::PartitionId& pid)
{
    assert(idx < totalCnt);
    assert(!configName.empty());
    *pid.mutable_buildid() = id;
    pid.mutable_buildid()->set_appname(appName); // all agent roleNode use appName as unified appName
    pid.set_role(ROLE_AGENT);

    autil::PartitionRange range;
    autil::RangeUtil::getRange(totalCnt, idx, range);
    pid.mutable_range()->set_from(range.first);
    pid.mutable_range()->set_to(range.second);
    pid.set_taskid(configName);
    *pid.add_clusternames() = id.appname();
}

bool RoleNameGenerator::parseAgentRoleName(const std::string& roleName, const std::string& appName, BuildId& id,
                                           std::string& configName, size_t& partCnt, size_t& idx)
{
    // partCnt will be set to 65536 by force when agent node use dynamic mapping
    proto::PartitionId pid;
    if (!proto::ProtoUtil::roleIdToPartitionId(roleName, appName, pid)) {
        return false;
    }

    if (pid.clusternames_size() != 1) {
        return false;
    }
    configName = pid.taskid();
    string originAppName = pid.clusternames()[0];
    id = pid.buildid();
    id.set_appname(originAppName);

    autil::PartitionRange range;
    range.first = pid.range().from();
    range.second = pid.range().to();

    double tmp = (double)(autil::RangeUtil::MAX_PARTITION_RANGE + 1) / (range.second - range.first + 1);
    partCnt = round(tmp);
    idx = autil::RangeUtil::getRangeIdx(0, autil::RangeUtil::MAX_PARTITION_RANGE, partCnt, range);
    return true;
}

}} // namespace build_service::proto
