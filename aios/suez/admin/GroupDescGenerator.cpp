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
#include "suez/admin/GroupDescGenerator.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>

#include "aios/network/http_arpc/ProtoJsonizer.h"
#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "carbon/legacy/RoleManagerWrapperInternal.h"
#include "hippo/proto/Common.pb.h"
#include "suez/admin/RangeParser.h"
#include "suez/admin/SignatureGenerator.h"
#include "suez/sdk/CmdLineDefine.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace http_arpc;
using namespace carbon;
using namespace carbon::proto;

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, GroupDescGenerator);

namespace suez {

const string GroupDescGenerator::VIP_STATUS_PATH = "../suez_worker/SearchService/vip_status";
const string GroupDescGenerator::CM2_STATUS_PATH = "../suez_worker/SearchService/cm2_status";
const string GroupDescGenerator::ARMORY_STATUS_PATH = "";
const string GroupDescGenerator::CM2_TYPE = "ST_CM2";
const string GroupDescGenerator::VIP_TYPE = "ST_VIP";
const string GroupDescGenerator::ARMORY_TYPE = "ST_ARMORY";

const string GroupDescGenerator::DISK_SIZE = "disk_size";

struct TableDistribution {
    static const string NORMAL_MODE;
    static const string REPLICA_MODE;
};
const string TableDistribution::NORMAL_MODE = "normal";
const string TableDistribution::REPLICA_MODE = "replica";

struct SwitchMode {
    static const string ROW;
    static const string COLUMN;
};
const string SwitchMode::ROW = "row";
const string SwitchMode::COLUMN = "column";

bool GroupDescGenerator::getGroupNameForSystemInfo(const JsonMap &jsonMap,
                                                   const std::string &zoneName,
                                                   std::vector<std::string> &groupName) {
    try {
        auto zones = getField(&jsonMap, "zones");
        if (!zones) {
            return true;
        }
        auto zone = getField(zones, zoneName);
        if (!zone) {
            AUTIL_LOG(ERROR, "no zone [%s] found", zoneName.c_str());
            return false;
        }
        string switchMode = getField(zone, "switch_mode", SwitchMode::ROW);
        if (SwitchMode::ROW == switchMode) {
            groupName.push_back(zoneName);
        } else {
            uint32_t roleCount = getRoleCount(zone);
            for (uint32_t i = 0; i < roleCount; i++) {
                groupName.push_back(getGroupName(zoneName, i));
            }
        }
    } catch (const exception &e) {
        AUTIL_LOG(ERROR, "exception %s", e.what());
        return false;
    }
    return true;
}

void GroupDescGenerator::generateGroupDescs(const JsonMap &jsonMap, map<string, GroupDesc> &groups) {
    GroupDescGenerator::generateGroupDescs(jsonMap, nullptr, groups);
}

void GroupDescGenerator::generateGroupDescs(const JsonMap &jsonMap,
                                            const JsonMap *statusInfo,
                                            map<string, GroupDesc> &groups) {
    auto zones = getField(&jsonMap, "zones");
    for (const auto &zonePair : *zones) {
        string zoneName = zonePair.first;
        auto zone = AnyCast<const JsonMap>(&zonePair.second);
        auto hippoConf = getField(zone, "hippo_config");
        string groupId = getField(hippoConf, "group_id", zoneName);
        string schedulerName = getField(hippoConf, "scheduler_name", string(""));
        string schedulerConfig = getField(hippoConf, "scheduler_config", string(""));
        int32_t targetVersion = getField(zone, "target_version", -1);
        string userDefVersion = StringUtil::toString(targetVersion);
        string leaderElectionStrategyType = getField(zone, "leader_election_strategy_type", string(""));

        uint32_t roleCount = getRoleCount(zone);
        string switchMode = getField(zone, "switch_mode", SwitchMode::ROW);

        if (switchMode == SwitchMode::ROW) {
            GroupDesc groupDesc;
            groupDesc.groupId = groupId;
            groupDesc.stepPercent = getField(hippoConf, "step_percent", 10);
            groupDesc.schedulerName = schedulerName;
            groupDesc.schedulerConfig = schedulerConfig;
            for (uint32_t roleId = 0; roleId < roleCount; roleId++) {
                auto roleDesc =
                    generateRoleDesc(roleId, zoneName, hippoConf, userDefVersion, leaderElectionStrategyType);
                generateTargetAndSignature(zoneName, roleId, roleCount, zone, roleDesc, statusInfo);
                groupDesc.roles.push_back(roleDesc);
            }
            generateServiceDesc(hippoConf, groupDesc);
            updateGroupDesc(groupDesc, groups);
        } else {
            for (uint32_t roleId = 0; roleId < roleCount; roleId++) {
                GroupDesc groupDesc;
                groupDesc.groupId = getGroupName(groupId, roleId);
                groupDesc.stepPercent = getField(hippoConf, "step_percent", 10);
                groupDesc.schedulerName = schedulerName;
                groupDesc.schedulerConfig = schedulerConfig;
                auto roleDesc =
                    generateRoleDesc(roleId, zoneName, hippoConf, userDefVersion, leaderElectionStrategyType);
                generateTargetAndSignature(zoneName, roleId, roleCount, zone, roleDesc, statusInfo);
                groupDesc.roles.push_back(roleDesc);
                generateServiceDesc(hippoConf, groupDesc);
                updateGroupDesc(groupDesc, groups);
            }
        }
    }
}

void GroupDescGenerator::updateGroupDesc(const GroupDesc &groupDesc, map<string, GroupDesc> &groups) {
    const string &groupName = groupDesc.groupId;
    if (groups.find(groupName) == groups.end()) {
        groups[groupName] = groupDesc;
        return;
    }
    GroupDesc &existDesc = groups[groupName];
    existDesc.roles.insert(existDesc.roles.end(), groupDesc.roles.begin(), groupDesc.roles.end());
    existDesc.services.insert(existDesc.services.end(), groupDesc.services.begin(), groupDesc.services.end());
    existDesc.forceUpdate |= groupDesc.forceUpdate;
    if (existDesc.stepPercent > groupDesc.stepPercent) {
        AUTIL_LOG(WARN, "update step percent from [%d] to [%d]", existDesc.stepPercent, groupDesc.stepPercent);
        existDesc.stepPercent = groupDesc.stepPercent;
    }
}

string GroupDescGenerator::getGroupName(const std::string &groupId, uint32_t roleId) {
    return groupId + "." + StringUtil::toString(roleId);
}

uint32_t GroupDescGenerator::getRoleCount(const JsonMap *zoneConf) {
    int32_t roleCount = getField(zoneConf, "role_count", int32_t(0));
    if (0 == roleCount) {
        JsonMap tableInfo = getField(zoneConf, "table_info", JsonMap());
        roleCount = getMaxPartCount(&tableInfo);
    }
    if (0 > roleCount) {
        FORMAT_THROW("role count should bigger than zero [%d]", roleCount);
    }
    if (0 == roleCount) {
        roleCount = 1;
    }
    return roleCount;
}

void GroupDescGenerator::generateTargetAndSignature(const string &zoneName,
                                                    int32_t roleId,
                                                    int32_t roleCount,
                                                    const JsonMap *zoneConf,
                                                    RoleDescription &roleDesc,
                                                    const JsonNodeRef::JsonMap *statusInfo) {
    JsonMap zoneConfForPart = JsonNodeRef::copy(*zoneConf);
    generateTarget(zoneName, roleDesc, roleId, roleCount, &zoneConfForPart, statusInfo);
    if (zoneConfForPart.find(DISK_SIZE) == zoneConfForPart.end()) {
        zoneConfForPart[DISK_SIZE] = extractDiskSizeQuota(roleDesc);
    }
    string targetStr = ToJsonString(zoneConfForPart, true);

    vector<string> signatureConfig = getField(zoneConf, "signature_config", vector<string>());
    if (signatureConfig.empty()) {
        bool tableIncNeedRolling = getField(zoneConf, "table_inc_need_rolling", true);
        bool bizConfigNeedRolling = getField(zoneConf, "biz_config_need_rolling", true);
        bool indexRootNeedRolling = getField(zoneConf, "index_root_need_rolling", false);
        bool serviceInfoRolling = getField(zoneConf, "service_info_need_rolling", false);
        signatureConfig = SignatureGenerator::defaultSignatureRule(
            tableIncNeedRolling, bizConfigNeedRolling, indexRootNeedRolling, serviceInfoRolling);
    }
    string signatureStr;
    if (!SignatureGenerator::generateSignature(signatureConfig, &zoneConfForPart, signatureStr)) {
        FORMAT_THROW("generate signature for zone [%s], partition [%d] failed.", zoneName.c_str(), roleId);
    }
    roleDesc.set_custominfo(targetStr);
    roleDesc.set_signature(signatureStr);
    AUTIL_LOG(DEBUG,
              "signature for zone [%s] role [%d / %d] is [%s]",
              zoneName.c_str(),
              roleId,
              roleCount,
              signatureStr.c_str());
}

void GroupDescGenerator::generateTarget(const string &zoneName,
                                        const carbon::proto::RoleDescription &roleDesc,
                                        int32_t roleId,
                                        int32_t roleCount,
                                        JsonMap *zoneConfForPart,
                                        const JsonNodeRef::JsonMap *statusInfo) {
    zoneConfForPart->erase("hippo_config");
    zoneConfForPart->erase("config");
    auto allTableInfo = getField(zoneConfForPart, "table_info");
    JsonMap generatedTableInfo;
    for (const auto &tableIter : *allTableInfo) {
        const string &tableName = tableIter.first;
        const JsonMap &generationInfos = AnyCast<const JsonMap>(tableIter.second);
        JsonMap generatedGenerationInfo;
        for (const auto &genIter : generationInfos) {
            const string &genName = genIter.first;
            JsonMap genInfo = JsonNodeRef::copy(AnyCast<const JsonMap>(genIter.second));
            generatePartitionTarget(roleId, roleCount, &genInfo);
            if (!genInfo.empty()) {
                generatedGenerationInfo[genName] = genInfo;
            }
        }
        if (!generationInfos.empty()) {
            generatedTableInfo[tableName] = generatedGenerationInfo;
        }
    }
    (*zoneConfForPart)["table_info"] = generatedTableInfo;

    generateServiceInfo(zoneName, roleDesc.rolename(), roleId, roleCount, zoneConfForPart);
    if (statusInfo) {
        generateLocalCm2Config(zoneName, roleDesc, roleId, roleCount, zoneConfForPart, statusInfo);
    }
}

void GroupDescGenerator::generateServiceInfo(
    const string &zoneName, const string &roleName, int32_t roleId, int32_t roleCount, JsonNodeRef::JsonMap *zoneConf) {
    if (zoneConf->end() == zoneConf->find("service_info")) {
        (*zoneConf)["service_info"] = JsonMap();
    }
    auto serviceInfo = getField(zoneConf, "service_info");
    (*serviceInfo)["part_count"] = roleCount;
    (*serviceInfo)["part_id"] = roleId;
    (*serviceInfo)["role_name"] = roleName;
    (*serviceInfo)["zone_name"] = genServiceInfoZoneName(zoneName);
}

void GroupDescGenerator::generateLocalCm2Config(const string &zoneName,
                                                const RoleDescription &roleDesc,
                                                int32_t roleId,
                                                int32_t roleCount,
                                                JsonNodeRef::JsonMap *zoneConf,
                                                const JsonNodeRef::JsonMap *statusInfo) {
    if (zoneConf->end() == zoneConf->find("service_info")) {
        (*zoneConf)["service_info"] = JsonMap();
    }
    auto serviceInfo = getField(zoneConf, "service_info");
    auto cm2Config = JsonMap();
    if (generateCm2LocalInfo(roleDesc, statusInfo, &cm2Config)) {
        (*serviceInfo)["cm2_config"] = cm2Config;
    }
}

bool GroupDescGenerator::generateCm2LocalInfo(const RoleDescription &roleDesc,
                                              const JsonNodeRef::JsonMap *statusInfo,
                                              JsonNodeRef::JsonMap *cm2Config) {
    uint32_t grpcPort = -1;
    uint32_t tcpPort = -1;
    bool rewriteTcpPort = getPortFromExtraRoleConfig(roleDesc, "tcp_port", tcpPort);
    bool rewriteGrpcPort = getPortFromExtraRoleConfig(roleDesc, "grpc_port", grpcPort);
    JsonArray localConfigs;
    for (const auto &[groupId, groupStatusInfo] : *statusInfo) {
        auto &groupStatusInfoMap = AnyCast<JsonMap>(groupStatusInfo);
        for (const auto &[roleId, roleStatusInfo] : groupStatusInfoMap) {
            auto &roleStatusInfoArray = AnyCast<JsonArray>(roleStatusInfo);
            for (size_t i = 0; i < roleStatusInfoArray.size(); ++i) {
                JsonMap localConf = JsonNodeRef::copy(AnyCast<JsonMap>(roleStatusInfoArray[i]));
                if (rewriteTcpPort) {
                    localConf["tcp_port"] = tcpPort;
                }
                if (rewriteGrpcPort) {
                    localConf["grpc_port"] = grpcPort;
                }
                localConf["support_heartbeat"] = true;
                localConfigs.push_back(localConf);
            }
        }
    }
    (*cm2Config)["local"] = localConfigs;
    return true;
}

bool GroupDescGenerator::getPortFromExtraRoleConfig(const RoleDescription &roleDesc,
                                                    const string &portName,
                                                    uint32_t &port) {
    string portStr;
    port = -1;
    if (!getFieldFromExtraRoleConfig(roleDesc, portName, portStr)) {
        return false;
    }
    if (!StringUtil::fromString<uint32_t>(portStr, port)) {
        return false;
    }
    return true;
}

bool GroupDescGenerator::getFieldFromExtraRoleConfig(const RoleDescription &roleDesc,
                                                     const string &field,
                                                     string &value) {
    auto &extraRoleConfig = roleDesc.extraroleconfig();
    int count = extraRoleConfig.size();
    for (int i = 0; i < count; i++) {
        if (extraRoleConfig.Get(i).key() == field) {
            value = extraRoleConfig.Get(i).value();
            return true;
        }
    }
    return false;
}

string GroupDescGenerator::genServiceInfoZoneName(const string &zoneName) {
    vector<string> zoneNameVec = StringUtil::split(zoneName, "-");
    if (zoneNameVec.size() == 3) {
        return zoneNameVec[2];
    }
    return zoneName;
}

void GroupDescGenerator::generatePartitionTarget(int32_t roleId, int32_t roleCount, JsonMap *genInfo) {
    string tableDistriModeStr = getField<string>(genInfo, "distribute_mode", TableDistribution::REPLICA_MODE);

    JsonMap *partInfo = getField(genInfo, "partitions");
    int32_t partSize = partInfo->size();
    const auto &iter = genInfo->find("total_partition_count");
    if (iter != genInfo->end()) {
        int32_t expectPartCount = autil::legacy::json::JsonNumberCast<int32_t>(iter->second);
        if (expectPartCount != partSize) {
            AUTIL_LOG(WARN, "expect part count [%d], real part size [%d]", expectPartCount, partSize);
        }
    } else {
        (*genInfo)["total_partition_count"] = autil::legacy::Any(partSize);
    }
    if (tableDistriModeStr == TableDistribution::REPLICA_MODE) {
        if (1 == partSize) {
            return;
        }
    } else if (tableDistriModeStr != TableDistribution::NORMAL_MODE) {
        FORMAT_THROW("unknown table distribution mode %s", tableDistriModeStr.c_str());
    }

    typedef pair<pair<uint32_t, uint32_t>, pair<string, Any>> PartEleType;
    vector<PartEleType> partVec;
    for (const auto &iter : *partInfo) {
        partVec.push_back(make_pair(RangeParser::parse(iter.first), make_pair(iter.first, iter.second)));
    }
    sort(partVec.begin(), partVec.end(), [](const PartEleType &a, const PartEleType &b) {
        return a.first.first < b.first.first;
    });

    partInfo->clear();
    int32_t idx = roleId;
    while (idx < partSize) {
        pair<string, Any> item = partVec[idx].second;
        (*partInfo)[item.first] = item.second;
        idx += roleCount;
    }

    if (partInfo->empty()) {
        genInfo->clear(); // remove useless genInfo;
    }
}

RoleDescription GroupDescGenerator::generateRoleDesc(int32_t roleId,
                                                     const string &zoneName,
                                                     const JsonMap *hippoConf,
                                                     const string &userDefVersion,
                                                     const std::string &leaderElectionStrategyType) {
    auto roleDesc = getField(hippoConf, "role_desc");
    RoleDescription roleDescProto;
    JsonNodeRef::castFromJsonMap(*roleDesc, &roleDescProto);
    if (roleDescProto.processinfos_size() == 0) {
        FORMAT_THROW("no processInfos for zone %s ", zoneName.c_str());
    }

    auto roleDescResources = roleDesc->find("resources");
    if (roleDescResources == roleDesc->end()) {
        FORMAT_THROW("no resources for zone %s ", zoneName.c_str());
    }
    if (!IsJsonArray(roleDescResources->second)) {
        FORMAT_THROW("resources is not array for zone %s ", zoneName.c_str());
    }
    const JsonArray &roleResourceArray = AnyCast<JsonArray>(roleDescResources->second);
    if (roleResourceArray.size() > 1) {
        // size > 1 means may be have special resource config for some role
        // suezops promise it is align with every resource in resources
        roleDescProto.clear_resources();
        for (size_t i = 0; i < roleResourceArray.size(); i++) {
            auto &resourcesItem = AnyCast<JsonMap>(roleResourceArray[i]);
            if (resourcesItem.find("partition_ids") == resourcesItem.end()) {
                // default config
                auto resources = roleDescProto.add_resources();
                http_arpc::ProtoJsonizer::fromJsonMap(resourcesItem, resources);
            } else {
                bool hit = false;
                auto roleIds = resourcesItem.find("partition_ids");
                if (!IsJsonArray(roleIds->second)) {
                    FORMAT_THROW("resources[%lu] partition_ids is not array for zone %s ", i, zoneName.c_str());
                }
                const JsonArray &roleIdsArray = AnyCast<JsonArray>(roleIds->second);
                for (size_t j = 0; j < roleIdsArray.size(); j++) {
                    if (JsonNumberCast<int32_t>(roleIdsArray[j]) == roleId) {
                        // hit
                        roleDescProto.clear_resources();
                        hit = true;
                        auto resources = roleDescProto.add_resources();
                        http_arpc::ProtoJsonizer::fromJsonMap(resourcesItem, resources);
                        break;
                    }
                }
                if (hit)
                    break;
            }
        }
        if (roleDescProto.resources_size() == 0) {
            FORMAT_THROW("role[%d] resources not find for zone %s ", roleId, zoneName.c_str());
        }
    }

    string roleNamePrefix;
    if (roleDescProto.rolename().empty()) {
        roleNamePrefix = zoneName;
    } else {
        roleNamePrefix = zoneName + "_" + roleDescProto.rolename();
    }
    string partId = StringUtil::toString(roleId);
    string roleName = roleNamePrefix + "_partition_" + partId;
    roleDescProto.set_rolename(roleName);
    roleDescProto.set_userdefversion(userDefVersion);
    addRoleParams(roleName, zoneName, partId, roleDescProto, leaderElectionStrategyType);
    return roleDescProto;
}

void GroupDescGenerator::addRoleParams(const string &roleName,
                                       const string &zoneName,
                                       const string &partId,
                                       RoleDescription &partitionRoleDesc,
                                       const string &leaderElectionStrategyType) {
    for (int i = 0; i < partitionRoleDesc.processinfos_size(); i++) {
        auto processInfo = partitionRoleDesc.mutable_processinfos(i);
        auto roleNameParam = processInfo->add_args();
        roleNameParam->set_key("--env");
        roleNameParam->set_value(ROLE_NAME + "=" + roleName);
        auto zoneNameParam = processInfo->add_args();
        zoneNameParam->set_key("--env");
        zoneNameParam->set_value(ZONE_NAME + "=" + zoneName);
        auto partIdParam = processInfo->add_args();
        partIdParam->set_key("--env");
        partIdParam->set_value(PART_ID + "=" + partId);
        auto reloadFlagEnv = processInfo->add_envs();
        reloadFlagEnv->set_key(RS_ALLOW_RELOAD_BY_CONFIG);
        reloadFlagEnv->set_value("true");
        if (!leaderElectionStrategyType.empty()) {
            auto param = processInfo->add_args();
            param->set_key("--env");
            param->set_value(LEADER_ELECTION_STRATEGY_TYPE + "=" + leaderElectionStrategyType);
        }
    }
}

void GroupDescGenerator::generateServiceDesc(const JsonMap *hippoConf, GroupDesc &groupDesc) {
    JsonMap serviceDesc = getField(hippoConf, "service_desc", JsonMap());
    if (serviceDesc.empty()) {
        return;
    }
    for (size_t i = 0; i < groupDesc.roles.size(); i++) {
        string roleName = groupDesc.roles[i].rolename();
        PublishServiceRequest request;
        request.set_rolename(roleName);
        for (const auto &servicePair : serviceDesc) {
            auto jsonMap = AnyCast<JsonMap>(servicePair.second);
            *request.add_serviceconfigs() = generateServiceConfig(&jsonMap);
        }
        groupDesc.services.push_back(request);
    }
}

ServiceConfig GroupDescGenerator::generateServiceConfig(const JsonMap *serviceMap) {
    string typeStr = getField(serviceMap, "type", string());
    string statusPath;
    if (typeStr == CM2_TYPE) {
        statusPath = CM2_STATUS_PATH;
    } else if (typeStr == VIP_TYPE) {
        statusPath = VIP_STATUS_PATH;
    } else if (typeStr == ARMORY_TYPE) {
        statusPath = ARMORY_STATUS_PATH;
    } else {
        statusPath = "";
        // FORMAT_THROW("type info is unknown: %s", ToJsonString(*serviceMap, true).c_str());
    }
    auto copyServiceMap = *serviceMap;
    copyServiceMap["statusPath"] = statusPath;
    auto configStrJsonMap = AnyCast<JsonMap>(copyServiceMap["configStr"]);
    if (configStrJsonMap.find("metaStr") != configStrJsonMap.end()) {
        const string *metaAny = AnyCast<string>(&configStrJsonMap["metaStr"]);
        if (metaAny == NULL) {
            copyServiceMap["metaStr"] = ToJsonString(configStrJsonMap["metaStr"]);
        } else {
            copyServiceMap["metaStr"] = configStrJsonMap["metaStr"];
        }
        configStrJsonMap.erase("metaStr");
    }
    copyServiceMap["configStr"] = ToJsonString(configStrJsonMap);

    ServiceConfig config;
    JsonNodeRef::castFromJsonMap(copyServiceMap, &config);
    return config;
}

uint32_t GroupDescGenerator::getMaxPartCount(const JsonMap *tableInfo) {
    uint32_t maxPartCount = 0;
    for (const auto &tableIter : *tableInfo) {
        const string &tableName = tableIter.first;
        const JsonMap &allGenInfo = AnyCast<JsonMap>(tableIter.second);
        for (const auto &genIter : allGenInfo) {
            const string &genName = genIter.first;
            const JsonMap &genInfo = AnyCast<JsonMap>(genIter.second);
            const JsonMap *partInfo = getField(&genInfo, "partitions");
            uint32_t partCount = partInfo->size();
            if (partCount > maxPartCount) {
                maxPartCount = partCount;
            }
            AUTIL_LOG(DEBUG,
                      "partition count in generation [%s] table [%s] is [%d].",
                      genName.c_str(),
                      tableName.c_str(),
                      partCount);
        }
    }
    return maxPartCount;
}

int32_t GroupDescGenerator::extractDiskSizeQuota(const carbon::proto::RoleDescription &roleDesc) {
    for (int i = 0; i < roleDesc.resources_size(); i++) {
        const auto &slotResource = roleDesc.resources(i);
        for (int j = 0; j < slotResource.resources_size(); j++) {
            if (slotResource.resources(j).name().find(DISK_SIZE) == 0) {
                return slotResource.resources(j).amount();
            }
        }
    }
    return numeric_limits<int32_t>::max();
}

} // namespace suez
