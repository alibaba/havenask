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
#include "swift/common/PathDefine.h"

#include <iosfwd>
#include <memory>
#include <stdlib.h>
#include <vector>

#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"

namespace swift {
namespace common {
AUTIL_LOG_SETUP(swift, PathDefine);

using namespace autil;
using namespace std;
using namespace fslib::fs;
// zk
const string HEARTBEAT_DIRECTORY = "heartbeat";
const string TASK_INFO_PATH = "tasks";
const string ADMIN_LOCK_DIR = "lock/admin";
const string BROKER_LOCK_DIR = "lock/broker";
const string LEADER_ELECTION = "LeaderElection/leader_election0000000000";
const string TOPIC_DIRECTORY = "topics";
const string TOPIC_META_NAME = "topic_meta";
const string PARTITION_INFO_NAME = "partition_info";
const string LEADER_INFO_FILE = "admin/leader_info";
const string BROKER_VERSIONS_INFO_FILE = "broker_version_info";
const string LEADER_INFO_JSON_FILE = "admin/leader_info.json";
const string LEADER_HISTORY_FILE = "admin/leader_history";
const string CONFIG_VERSION_FILE = "config/version";
const string ROLE_ADDRESS_SEPARATOR = "#_#";
const string GROUP_ROLE_SEPARATOR = "##";
const string SCHEMA_DIRECTORY = "schema";
const string TOPIC_RW_FILE = "topic_rwinfo";
const string NOUSE_TOPIC_DIRECTORY = "nouse_topics";
const string CHANGE_PARTCNT_TASK_FILE = "change_partcnt_tasks";
const string WRITE_VERSION_CONTROL_DIR = "write_version_control";
const string BROKER_HEALTH_CHECK_DIR = "_status_";
const string SELF_MASTER_VERSION_FILE = "self_master_version";
const string ROLE_TYPE_ADMIN = "admin";
const string ROLE_TYPE_BROKER = "broker";
const string EMPTY_STRING = "";
const string CLEAN_AT_DELETE_TASK_FILE = "clean_at_delete_tasks";
const string TOPIC_AUTH_FILE = "auth/topic_auth";

// dfs
const string PORT_FILE = "ports";
const string PUBLISH_INLINE_FILE = "inline";

string PathDefine::getAddress(const string &ip, uint16_t port) {

    string tmpIp = ip;
    StringUtil::trim(tmpIp);
    return tmpIp + ":" + StringUtil::toString(port);
}

string PathDefine::getRoleName(const string &group, const string &name) { return group + GROUP_ROLE_SEPARATOR + name; }

string PathDefine::getRoleAddress(const string &roleName, const string &ip, uint16_t port) {
    return roleName + ROLE_ADDRESS_SEPARATOR + getAddress(ip, port);
}
string PathDefine::getRoleAddress(const string &roleName, const string &address) {
    return roleName + ROLE_ADDRESS_SEPARATOR + address;
}

bool PathDefine::parseRoleAddress(const string &roleAddress, string &roleName, string &address) {
    size_t pos = roleAddress.find(ROLE_ADDRESS_SEPARATOR);
    if (pos != string::npos) {
        roleName = roleAddress.substr(0, pos);
        address = roleAddress.substr(pos + ROLE_ADDRESS_SEPARATOR.size());
        return true;
    }
    return false;
}

bool PathDefine::parseRoleGroup(const string &roleName, string &group, string &name) {
    size_t pos = roleName.find(GROUP_ROLE_SEPARATOR);
    if (pos != string::npos) {
        group = roleName.substr(0, pos);
        name = roleName.substr(pos + GROUP_ROLE_SEPARATOR.size());
        return true;
    }
    return false;
}

string PathDefine::parseRole(const string &roleAddress) {
    size_t pos = roleAddress.find(ROLE_ADDRESS_SEPARATOR);
    string roleName;
    if (pos != string::npos) {
        roleName = roleAddress.substr(0, pos);
    }
    return roleName;
}

bool PathDefine::parseAddress(const string &address, string &ip, uint16_t &port) {
    string tmp = address;
    StringUtil::trim(tmp);
    if (StringUtil::startsWith(tmp, "tcp:")) {
        tmp = tmp.substr(4);
    }
    size_t pos = tmp.find(":");
    if (pos == string::npos || pos < 1) {
        return false;
    }
    string portStr = tmp.substr(pos + 1);
    uint16_t tmpPort = 0;
    if (!StringUtil::strToUInt16(portStr.c_str(), tmpPort) || StringUtil::toString(tmpPort) != portStr) {
        return false;
    }
    ip = tmp.substr(0, pos);
    port = tmpPort;

    return true;
}

bool PathDefine::parseRoleGroupAndIndex(const string &roleName, string &group, uint32_t &index) {
    size_t pos = roleName.find(GROUP_ROLE_SEPARATOR);
    if (pos != string::npos) {
        group = roleName.substr(0, pos);
        string left = roleName.substr(pos + GROUP_ROLE_SEPARATOR.size());
        const vector<string> &names = StringUtil::split(left, "_");
        if (names.size() <= 2) {
            return false;
        }
        if (!StringUtil::fromString(names[1], index)) {
            return false;
        }
        return true;
    }
    return false;
}

const std::string &PathDefine::parseRoleType(const std::string &roleName) {
    if (roleName.find(ROLE_TYPE_ADMIN) != string::npos) {
        return ROLE_TYPE_ADMIN;
    } else if (roleName.find(ROLE_TYPE_BROKER) != string::npos) {
        return ROLE_TYPE_BROKER;
    } else {
        return EMPTY_STRING;
    }
}

string PathDefine::heartbeatReportAddress(const string &zkRoot, const string &workerAddress) {
    string tmp = workerAddress;
    StringUtil::trim(tmp);
    return FileSystem::joinFilePath(heartbeatMonitorAddress(zkRoot), tmp);
}

string PathDefine::heartbeatMonitorAddress(const string &zkRoot) {
    return FileSystem::joinFilePath(zkRoot, HEARTBEAT_DIRECTORY);
}

string PathDefine::getAdminLockDir(const string &zkRoot) { return FileSystem::joinFilePath(zkRoot, ADMIN_LOCK_DIR); }

string PathDefine::getBrokerLockDir(const string &zkRoot, string roleName) {
    string path = FileSystem::joinFilePath(zkRoot, BROKER_LOCK_DIR);
    return FileSystem::joinFilePath(path, roleName);
}

string PathDefine::getLeaderFilePath(const string &zkRoot, const string &roleName) {
    const auto &roleType = parseRoleType(roleName);
    if (ROLE_TYPE_ADMIN == roleType) {
        return FileSystem::joinFilePath(getAdminLockDir(zkRoot), LEADER_ELECTION);
    } else if (ROLE_TYPE_BROKER == roleType) {
        return FileSystem::joinFilePath(getBrokerLockDir(zkRoot, roleName), LEADER_ELECTION);
    } else {
        return EMPTY_STRING;
    }
}

string PathDefine::getAdminDir(const string &zkRoot) { return FileSystem::joinFilePath(zkRoot, "admin"); }

string PathDefine::getTaskDir(const string &zkRoot) { return FileSystem::joinFilePath(zkRoot, TASK_INFO_PATH); }

string PathDefine::getTaskInfoPath(const string &zkRoot, const string &roleName) {
    return FileSystem::joinFilePath(getTaskDir(zkRoot), roleName);
}

string PathDefine::getTopicDir(const string &zkRoot) { return FileSystem::joinFilePath(zkRoot, TOPIC_DIRECTORY); }

string PathDefine::getTopicDir(const string &zkRoot, const string &topicName) {
    return FileSystem::joinFilePath(getTopicDir(zkRoot), topicName);
}

string PathDefine::getTopicPartitionDir(const string &zkRoot, const string &topicName) {
    return FileSystem::joinFilePath(getTopicDir(zkRoot, topicName), "partitions");
}

string PathDefine::getSchemaDir(const string &zkRoot) { return FileSystem::joinFilePath(zkRoot, SCHEMA_DIRECTORY); }

string PathDefine::getTopicSchemaFile(const string &zkRoot, const string &topicName) {
    const string &path = PathDefine::getSchemaDir(zkRoot);
    return FileSystem::joinFilePath(path, topicName);
}

string PathDefine::getPartitionInfoFile(const string &zkRoot) {
    return FileSystem::joinFilePath(zkRoot, PARTITION_INFO_NAME);
}

string PathDefine::getTopicMetaFile(const string &zkRoot) { return FileSystem::joinFilePath(zkRoot, TOPIC_META_NAME); }

string PathDefine::getChangePartCntTaskFile(const string &zkRoot) {
    return FileSystem::joinFilePath(zkRoot, CHANGE_PARTCNT_TASK_FILE);
}

string PathDefine::getTopicMetaPath(const string &zkRoot, const string &topicName) {
    return FileSystem::joinFilePath(getTopicDir(zkRoot, topicName), "meta");
}

string PathDefine::getTopicPartitionPath(const string &zkRoot, const string &topicName, uint32_t partitionId) {
    return FileSystem::joinFilePath(getTopicPartitionDir(zkRoot, topicName), StringUtil::toString(partitionId));
}

string PathDefine::getLeaderInfoFile(const string &zkRoot) {
    return FileSystem::joinFilePath(zkRoot, LEADER_INFO_FILE);
}

string PathDefine::getLeaderInfoJsonFile(const string &zkRoot) {
    return FileSystem::joinFilePath(zkRoot, LEADER_INFO_JSON_FILE);
}

string PathDefine::getLeaderHistoryFile(const string &zkRoot) {
    return FileSystem::joinFilePath(zkRoot, LEADER_HISTORY_FILE);
}

string PathDefine::getConfigVersionFile(const std::string &zkRoot) {
    return FileSystem::joinFilePath(zkRoot, CONFIG_VERSION_FILE);
}

string PathDefine::getTopicRWFile(const string &zkRoot) { return FileSystem::joinFilePath(zkRoot, TOPIC_RW_FILE); }

string PathDefine::generateAccessKey(const string &ip) {
    string slotId;
    string workDir;
    if (!autil::EnvUtil::getEnvWithoutDefault("HIPPO_APP_SLOT_ID", slotId)) {
        AUTIL_LOG(WARN, "get env HIPPO_APP_SLOT_ID failed!");
    }
    if (!autil::EnvUtil::getEnvWithoutDefault("HIPPO_APP_WORKDIR", workDir)) {
        AUTIL_LOG(WARN, "get env HIPPO_APP_WORKDIR failed!");
    }
    return ip + "_" + slotId + "_" + workDir;
}

std::string PathDefine::getBrokerVersionsFile(const string &zkRoot) {
    return FileSystem::joinFilePath(zkRoot, BROKER_VERSIONS_INFO_FILE);
}

string PathDefine::getNoUseTopicDir(const string &zkRoot) {
    return FileSystem::joinFilePath(zkRoot, NOUSE_TOPIC_DIRECTORY);
}

string PathDefine::getWriteVersionControlPath(const string &zkRoot) {
    return FileSystem::joinFilePath(zkRoot, WRITE_VERSION_CONTROL_DIR);
}

// zkPath: zfs://127.0.0.1:2181/path
#define ZFS_PREFIX "zfs://"
#define ZFS_PREFIX_LEN 6

string PathDefine::getPathFromZkPath(const std::string &zkPath) {
    if (!StringUtil::startsWith(zkPath, ZFS_PREFIX)) {
        AUTIL_LOG(ERROR, "zkPath[%s] is invalid", zkPath.c_str());
        return "";
    }
    string tmpStr = zkPath.substr(ZFS_PREFIX_LEN);
    string::size_type pos = tmpStr.find("/");
    if (pos == string::npos) {
        return "";
    }
    return tmpStr.substr(pos);
}

bool PathDefine::writeRecommendPort(uint32_t rpcPort, uint32_t httpPort) {
    string portStr = StringUtil::toString(rpcPort) + "," + StringUtil::toString(httpPort);
    if (fslib::EC_OK != FileSystem::writeFile(PORT_FILE, portStr)) {
        AUTIL_LOG(WARN, "write[%d %d] to[%s] fail", rpcPort, httpPort, PORT_FILE.c_str());
        return false;
    }
    return true;
}

bool PathDefine::readRecommendPort(uint32_t &rpcPort, uint32_t &httpPort) {
    string portStr;
    if (fslib::EC_OK != FileSystem::readFile(PORT_FILE, portStr)) {
        AUTIL_LOG(WARN, "%s not exist, will use random port", PORT_FILE.c_str());
        return false;
    }
    const vector<string> &ports = StringUtil::split(portStr, ",");
    if (2 == ports.size()) {
        if (StringUtil::strToUInt32(ports[0].c_str(), rpcPort) && StringUtil::strToUInt32(ports[1].c_str(), httpPort)) {
            return true;
        }
    }
    AUTIL_LOG(INFO, "port file[%s] content[%s] illegal, ignore", PORT_FILE.c_str(), portStr.c_str());
    return false;
}

string PathDefine::getHealthCheckDir(const string &root) {
    return FileSystem::joinFilePath(root, BROKER_HEALTH_CHECK_DIR);
}

string PathDefine::getHealthCheckVersionDir(const string &root, const string &version) {
    return FileSystem::joinFilePath(getHealthCheckDir(root), version);
}

string PathDefine::getHealthCheckFile(const string &root, const string &version, const string &roleName) {
    return FileSystem::joinFilePath(getHealthCheckVersionDir(root, version), roleName);
}

string PathDefine::getSelfMasterVersionFile(const string &zkRoot) {
    return FileSystem::joinFilePath(zkRoot, SELF_MASTER_VERSION_FILE);
}

string PathDefine::getPublishInlineFile(const string &rootPath) {
    return FileSystem::joinFilePath(rootPath, PUBLISH_INLINE_FILE);
}

string PathDefine::getCleanAtDeleteTaskFile(const string &zkRoot) {
    return FileSystem::joinFilePath(zkRoot, CLEAN_AT_DELETE_TASK_FILE);
}

string PathDefine::getTopicAclDataFile(const string &zkRoot) {
    return FileSystem::joinFilePath(zkRoot, TOPIC_AUTH_FILE);
}

} // namespace common
} // namespace swift
