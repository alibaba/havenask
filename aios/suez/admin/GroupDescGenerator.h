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
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "carbon/proto/Carbon.pb.h"
#include "suez/sdk/JsonNodeRef.h"

namespace carbon {
struct GroupDesc;
} // namespace carbon

namespace suez {

class GroupDescGenerator {
public:
    static bool getGroupNameForSystemInfo(const JsonNodeRef::JsonMap &jsonMap,
                                          const std::string &zoneName,
                                          std::vector<std::string> &groupName);
    static void generateGroupDescs(const JsonNodeRef::JsonMap &jsonMap,
                                   std::map<std::string, carbon::GroupDesc> &groupDesc);
    static void generateGroupDescs(const JsonNodeRef::JsonMap &jsonMap,
                                   const JsonNodeRef::JsonMap *statusInfo,
                                   std::map<std::string, carbon::GroupDesc> &groupDesc);

private:
    static uint32_t getMaxPartCount(const JsonNodeRef::JsonMap *tableInfo);
    static carbon::proto::RoleDescription generateRoleDesc(int32_t roleId,
                                                           const std::string &zoneName,
                                                           const JsonNodeRef::JsonMap *hippoConf,
                                                           const std::string &userDefVersion,
                                                           const std::string &leaderElectionStrategyType);
    static void addRoleParams(const std::string &roleName,
                              const std::string &zoneName,
                              const std::string &partId,
                              carbon::proto::RoleDescription &partitionRoleDesc,
                              const std::string &leaderElectionStrategyType);
    static uint32_t getRoleCount(const JsonNodeRef::JsonMap *zoneConf);
    static std::string getGroupName(const std::string &groupId, uint32_t roleId);
    static void generateServiceDesc(const JsonNodeRef::JsonMap *hippoConf, carbon::GroupDesc &groupDesc);
    static void generateTargetAndSignature(const std::string &zoneName,
                                           int32_t roleId,
                                           int32_t roleCount,
                                           const JsonNodeRef::JsonMap *zoneConf,
                                           carbon::proto::RoleDescription &roleDesc,
                                           const JsonNodeRef::JsonMap *statusInfo);

    static void generateTarget(const std::string &zoneName,
                               const carbon::proto::RoleDescription &roleDesc,
                               int32_t partId,
                               int32_t roleCount,
                               JsonNodeRef::JsonMap *copyZoneConf,
                               const JsonNodeRef::JsonMap *statusInfo);
    static void generatePartitionTarget(int32_t partId, int32_t roleCount, JsonNodeRef::JsonMap *genInfo);
    static void generateServiceInfo(const std::string &zoneName,
                                    const std::string &roleName,
                                    int32_t roleId,
                                    int32_t roleCount,
                                    JsonNodeRef::JsonMap *zoneConf);
    static void generateLocalCm2Config(const std::string &zoneName,
                                       const carbon::proto::RoleDescription &roleDesc,
                                       int32_t roleId,
                                       int32_t roleCount,
                                       JsonNodeRef::JsonMap *zoneConf,
                                       const JsonNodeRef::JsonMap *statusInfo);
    static carbon::proto::ServiceConfig generateServiceConfig(const JsonNodeRef::JsonMap *serviceMap);
    static void updateGroupDesc(const carbon::GroupDesc &groupDesc, std::map<std::string, carbon::GroupDesc> &groups);
    static int32_t extractDiskSizeQuota(const carbon::proto::RoleDescription &roleDesc);
    static bool generateCm2LocalInfo(const carbon::proto::RoleDescription &roleDesc,
                                     const JsonNodeRef::JsonMap *statusInfo,
                                     JsonNodeRef::JsonMap *cm2Config);
    static bool getFieldFromExtraRoleConfig(const carbon::proto::RoleDescription &roleDesc,
                                            const std::string &field,
                                            std::string &value);
    static bool getPortFromExtraRoleConfig(const carbon::proto::RoleDescription &roleDesc,
                                           const std::string &portName,
                                           uint32_t &port);

private:
    static const std::string VIP_STATUS_PATH;
    static const std::string CM2_STATUS_PATH;
    static const std::string ARMORY_STATUS_PATH;
    static const std::string CM2_TYPE;
    static const std::string VIP_TYPE;
    static const std::string ARMORY_TYPE;

    static const std::string DISK_SIZE;

public:
    static std::string genServiceInfoZoneName(const std::string &zoneName);

private:
    GroupDescGenerator();
    ~GroupDescGenerator();
    GroupDescGenerator(const GroupDescGenerator &);
    GroupDescGenerator &operator=(const GroupDescGenerator &);
};

} // namespace suez
