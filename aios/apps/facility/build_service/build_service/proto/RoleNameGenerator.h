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
#ifndef ISEARCH_BS_ROLENAMEGENERATOR_H
#define ISEARCH_BS_ROLENAMEGENERATOR_H

#include "build_service/common_define.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/Log.h"

namespace build_service { namespace proto {

typedef std::pair<std::string, std::string> GroupRolePair; // <group, role>

class RoleNameGenerator
{
public:
    RoleNameGenerator();
    ~RoleNameGenerator();
    RoleNameGenerator(const RoleNameGenerator&) = delete;
    RoleNameGenerator& operator=(const RoleNameGenerator&) = delete;

    static std::vector<GroupRolePair> generateGroupRolePairs(const std::vector<PartitionId>& partitionId);
    static GroupRolePair generateGroupRolePair(const PartitionId& partitionId);
    static std::string generateGroupName(const PartitionId& partitionId);
    static std::string generateRoleGroupName(const PartitionId& partitionId);
    static std::string generateRoleName(const PartitionId& partitionId);
    static std::string generateGangIdentifier(const PartitionId& partitionId);

    static void generateAgentNodePartitionId(const BuildId& id, const std::string& appName,
                                             const std::string& configName, size_t totalCnt, size_t idx,
                                             PartitionId& pid);

    static bool parseAgentRoleName(const std::string& roleName, const std::string& appName, BuildId& id,
                                   std::string& configName, size_t& partCnt, size_t& idx);

private:
    static void fillTaskItems(const PartitionId& partitionId, std::vector<std::string>& items);

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(RoleNameGenerator);

}} // namespace build_service::proto

#endif // ISEARCH_BS_ROLENAMEGENERATOR_H
