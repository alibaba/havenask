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

#include <stdint.h>
#include <string>

#include "autil/Log.h"

namespace swift {
namespace common {

class PathDefine {
public:
    static std::string getAddress(const std::string &ip, uint16_t port);
    static std::string getRoleName(const std::string &group, const std::string &name);
    static std::string getRoleAddress(const std::string &roleName, const std::string &ip, uint16_t port);
    static std::string getRoleAddress(const std::string &roleName, const std::string &address);
    static bool parseRoleAddress(const std::string &roleAddress, std::string &roleName, std::string &address);
    static std::string parseRole(const std::string &roleAddress);
    static bool parseAddress(const std::string &address, std::string &ip, uint16_t &port);
    static bool parseRoleGroup(const std::string &roleName, std::string &group, std::string &name);
    static bool parseRoleGroupAndIndex(const std::string &roleName, std::string &group, uint32_t &index);
    static const std::string &parseRoleType(const std::string &roleName);
    static std::string heartbeatReportAddress(const std::string &zkRoot, const std::string &workerAddress);
    static std::string heartbeatMonitorAddress(const std::string &zkRoot);
    static std::string getAdminLockDir(const std::string &zkRoot);
    static std::string getBrokerLockDir(const std::string &zkRoot, std::string role = "");
    static std::string getLeaderFilePath(const std::string &zkRoot, const std::string &roleName);
    static std::string getAdminDir(const std::string &zkRoot);

    static std::string getTaskDir(const std::string &zkRoot);
    static std::string getTaskInfoPath(const std::string &zkRoot, const std::string &roleName);

    static std::string getTopicDir(const std::string &zkRoot);
    static std::string getTopicDir(const std::string &zkRoot, const std::string &topicName);
    static std::string getTopicPartitionDir(const std::string &zkRoot, const std::string &topicName);
    static std::string getSchemaDir(const std::string &zkRoot);
    static std::string getTopicSchemaFile(const std::string &zkRoot, const std::string &topicName);
    static std::string getNoUseTopicDir(const std::string &zkRoot);

    // new
    static std::string getPartitionInfoFile(const std::string &zkRoot);
    static std::string getTopicMetaFile(const std::string &zkRoot);
    static std::string getChangePartCntTaskFile(const std::string &zkRoot);
    // old
    static std::string getTopicMetaPath(const std::string &zkRoot, const std::string &topicName);
    static std::string
    getTopicPartitionPath(const std::string &zkRoot, const std::string &topicName, uint32_t partitionId);

    static std::string getLeaderInfoFile(const std::string &zkRoot);
    static std::string getLeaderInfoJsonFile(const std::string &zkRoot);
    static std::string getLeaderHistoryFile(const std::string &zkRoot);

    static std::string getConfigVersionFile(const std::string &zkRoot);
    static std::string generateAccessKey(const std::string &ip);
    static std::string getBrokerVersionsFile(const std::string &zkRoot);
    static std::string getTopicRWFile(const std::string &zkRoot);
    static std::string getWriteVersionControlPath(const std::string &zkRoot);

    static std::string getPathFromZkPath(const std::string &zkPath);
    static bool writeRecommendPort(uint32_t rpcPort, uint32_t httpPort);
    static bool readRecommendPort(uint32_t &rpcPort, uint32_t &httpPort);
    static std::string getHealthCheckDir(const std::string &root);
    static std::string getHealthCheckVersionDir(const std::string &root, const std::string &version);
    static std::string
    getHealthCheckFile(const std::string &root, const std::string &version, const std::string &roleName);

    static std::string getSelfMasterVersionFile(const std::string &zkRoot);
    static std::string getPublishInlineFile(const std::string &rootPath);
    static std::string getCleanAtDeleteTaskFile(const std::string &rootPath);
    static std::string getTopicAclDataFile(const std::string &zkRoot);

private:
    PathDefine();
    ~PathDefine();
    PathDefine(const PathDefine &);
    PathDefine &operator=(const PathDefine &);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace common
} // namespace swift
