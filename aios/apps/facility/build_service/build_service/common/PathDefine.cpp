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
#include "build_service/common/PathDefine.h"

#include <errno.h>
#include <limits.h>
#include <unistd.h>

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "build_service/proto/ProcessorTaskIdentifier.h"
#include "build_service/proto/ProtoUtil.h"
#include "fslib/fslib.h"
#include "fslib/util/FileUtil.h"
using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;
using namespace build_service::util;
using namespace build_service::proto;

namespace build_service { namespace common {
BS_LOG_SETUP(common, PathDefine);

#define ZFS_PREFIX "zfs://"
#define ZFS_PREFIX_LEN 6

const string PathDefine::ZK_GENERATION_DIR_PREFIX = "generation";
const string PathDefine::ZK_GENERATION_STATUS_FILE = "generation_status";
const string PathDefine::ZK_SCHEDULE_TIMESTAMP_FILE = "bsadmin_schedule_timestamp";
const string PathDefine::ZK_GENERATION_STATUS_STOP_FILE = "generation_status.stopped";
const string PathDefine::ZK_ADMIN_DIR = "admin";
const string PathDefine::ZK_AGENT_NODE_DIR = "agent_nodes";
const string PathDefine::ZK_AGENT_BLACK_LIST_FILE = "agent_black_list_info";
const string PathDefine::ZK_ADMIN_CONTROL_FLOW_DIR = "control_flow";
const string PathDefine::ZK_CURRENT_STATUS_FILE = "current";
const string PathDefine::ZK_TARGET_STATUS_FILE = "target";
const string PathDefine::INDEX_PARTITION_LOCK_DIR_NAME = "__index_lock__";
const string PathDefine::ZK_GENERATION_STATUS_FILE_SPLIT = ".";
const string PathDefine::ZK_GENERATION_CHECKPOINT_DIR = "checkpoints";
const string PathDefine::CLUSTER_CHECKPOINT_PREFIX = "version";
const string PathDefine::RESERVED_CHECKPOINT_FILENAME = "reserved_checkpoints";
const string PathDefine::SNAPSHOT_CHECKPOINT_FILENAME = "snapshot_checkpoints";
const string PathDefine::CHECK_POINT_STATUS_FILE = "checkpoint_status";
const string PathDefine::ZK_GENERATION_INDEX_INFOS = "generation_index_infos";
const string PathDefine::ZK_SERVICE_INFO_TEMPLATE = "service_info_template";
const string PathDefine::WORKER_RUNTIMEDATA_PATH = "runtimedata";

// string PathDefine::getPartitionCurrentStatusZkPath(
//         const string &zkRoot, const PartitionId &pid)
// {
//     return fslib::util::FileUtil::joinFilePath(getPartitionZkRoot(zkRoot, pid), ZK_CURRENT_STATUS_FILE);
// }

// string PathDefine::getPartitionTargetStatusZkPath(
//         const string &zkRoot, const PartitionId &pid)
// {
//     return fslib::util::FileUtil::joinFilePath(getPartitionZkRoot(zkRoot, pid), ZK_TARGET_STATUS_FILE);
// }

string PathDefine::getAdminZkRoot(const string& zkRoot)
{
    return fslib::util::FileUtil::joinFilePath(zkRoot, ZK_ADMIN_DIR);
}

string PathDefine::getAgentNodeRoot(const string& zkRoot)
{
    return fslib::util::FileUtil::joinFilePath(zkRoot, ZK_AGENT_NODE_DIR);
}

string PathDefine::getAdminScheduleTimestampFilePath(const string& zkRoot)
{
    return fslib::util::FileUtil::joinFilePath(zkRoot, ZK_SCHEDULE_TIMESTAMP_FILE);
}

string PathDefine::getAdminControlFlowRoot(const string& zkRoot)
{
    return fslib::util::FileUtil::joinFilePath(zkRoot, ZK_ADMIN_CONTROL_FLOW_DIR);
}

string PathDefine::getGenerationZkRoot(const string& zkRoot, const BuildId& buildId)
{
    return fslib::util::FileUtil::joinFilePath(zkRoot, ZK_GENERATION_DIR_PREFIX + ZK_GENERATION_STATUS_FILE_SPLIT +
                                                           buildId.appname() + ZK_GENERATION_STATUS_FILE_SPLIT +
                                                           buildId.datatable() + ZK_GENERATION_STATUS_FILE_SPLIT +
                                                           StringUtil::toString(buildId.generationid()));
}

string PathDefine::getGenerationStatusFile(const std::string& zkRoot, const BuildId& buildId)
{
    return fslib::util::FileUtil::joinFilePath(getGenerationZkRoot(zkRoot, buildId), ZK_GENERATION_STATUS_FILE);
}

string PathDefine::getGenerationStopFile(const std::string& zkRoot, const proto::BuildId& buildId)
{
    return fslib::util::FileUtil::joinFilePath(getGenerationZkRoot(zkRoot, buildId), ZK_GENERATION_STATUS_STOP_FILE);
}

string PathDefine::getPartitionZkRoot(const string& zkRoot, const PartitionId& pid, bool ignoreBackupId)
{
    // DO NOT CHANGE ZKROOT???
    string roleId;
    ProtoUtil::partitionIdToRoleId(pid, roleId, ignoreBackupId);

    // agent node root not use generation zk path
    if (pid.role() == ROLE_AGENT) {
        string agentRootDir = getAgentNodeRoot(zkRoot);
        return fslib::util::FileUtil::joinFilePath(agentRootDir, roleId);
    }

    string generationRoot = getGenerationZkRoot(zkRoot, pid.buildid());
    return fslib::util::FileUtil::joinFilePath(generationRoot, roleId);
}

bool PathDefine::getAllGenerations(const std::string& path, vector<BuildId>& buildIds)
{
    vector<string> fileList;
    ErrorCode ec = FileSystem::listDir(path, fileList);
    if (ec != fslib::EC_OK) {
        string errorMsg = "List dir failed, path [" + path + "], error[" + FileSystem::getErrorString(ec) + "]";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    for (size_t i = 0; i < fileList.size(); i++) {
        string statusFilePath = fslib::util::FileUtil::joinFilePath(path, fileList[i]);
        ec = FileSystem::isDirectory(statusFilePath);
        if (ec == EC_FALSE) {
            continue;
        } else if (ec != EC_TRUE) {
            string errorMsg =
                "is dir failed, path [" + statusFilePath + "], error[" + FileSystem::getErrorString(ec) + "]";
            BS_LOG(ERROR, "%s", errorMsg.c_str());
            return false;
        }
        vector<string> items = StringTokenizer::tokenize(StringView(fileList[i]), ZK_GENERATION_STATUS_FILE_SPLIT);
        if (items.size() == 4) {
        } else if (items.size() == 3) {
            items.insert(items.begin() + 1, "");
        } else {
            continue;
        }
        int32_t generationId;
        if (StringUtil::fromString(items[3].c_str(), generationId)) {
            BuildId buildId;
            buildId.set_appname(items[1]);
            buildId.set_datatable(items[2]);
            buildId.set_generationid(generationId);
            buildIds.push_back(buildId);
        }
    }
    return true;
}

string PathDefine::getLocalConfigPath()
{
    char buffer[PATH_MAX] = {'\0'};
    char* ret = getcwd(buffer, PATH_MAX);
    if (ret == NULL) {
        string errorMsg = "failed to getcwd, errmsg: " + string(strerror(errno));
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return "";
    }
    string cwd(ret);
    string configPath = fslib::util::FileUtil::joinFilePath(cwd, "config");
    bool exist = false;
    if (!fslib::util::FileUtil::isExist(configPath, exist) || !exist) {
        fslib::util::FileUtil::mkDir(configPath);
    }
    return configPath;
}
string PathDefine::getLocalDataPath()
{
    char buffer[PATH_MAX] = {'\0'};
    char* ret = getcwd(buffer, PATH_MAX);
    if (ret == NULL) {
        string errorMsg = "failed to getcwd, errmsg: " + string(strerror(errno));
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return "";
    }
    string cwd(ret);
    string runtimeDataPath = fslib::util::FileUtil::joinFilePath(cwd, WORKER_RUNTIMEDATA_PATH);
    bool exist = false;
    if (!fslib::util::FileUtil::isExist(runtimeDataPath, exist) || !exist) {
        fslib::util::FileUtil::mkDir(runtimeDataPath);
    }
    return runtimeDataPath;
}

string PathDefine::getLocalLuaScriptRootPath()
{
    char buffer[PATH_MAX] = {'\0'};
    char* ret = getcwd(buffer, PATH_MAX);
    if (ret == NULL) {
        string errorMsg = "failed to getcwd, errmsg: " + string(strerror(errno));
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return "";
    }
    string cwd(ret);
    string luaPath = fslib::util::FileUtil::joinFilePath(cwd, "lua_scripts");
    bool exist = false;
    if (!fslib::util::FileUtil::isExist(luaPath, exist) || !exist) {
        fslib::util::FileUtil::mkDir(luaPath);
    }
    return luaPath;
}

string PathDefine::getAmonitorNodePath(const PartitionId& pid)
{
    stringstream ss;
    ss << "generation_" << pid.buildid().generationid() << "/";

    if (pid.role() == ROLE_TASK) {
        string taskId = pid.taskid();
        TaskIdentifier taskIdentifier;
        if (taskIdentifier.fromString(taskId)) {
            string taskName;
            taskIdentifier.getTaskName(taskName);
            ss << "task_name-" << taskName << "/";
            ss << "task_id-" << taskIdentifier.getTaskId() << "/";
        }
    } else if (pid.role() == ROLE_MERGER) {
        ss << ProtoUtil::toRoleString(pid.role()) << "/";
        ss << pid.mergeconfigname() << "/";
    } else {
        ss << ProtoUtil::toRoleString(pid.role()) << "/";
        ss << ProtoUtil::toStepString(pid) << "/";
    }

    if (pid.role() == ROLE_BUILDER || pid.role() == ROLE_MERGER || pid.role() == ROLE_TASK) {
        assert(pid.clusternames_size() == 1);
        ss << pid.clusternames(0) << "/";
    } else if (pid.role() == ROLE_PROCESSOR) {
        ss << pid.buildid().datatable() << "/";
        ProcessorTaskIdentifier identifier;
        if (identifier.fromString(pid.taskid())) {
            string clusterName;
            identifier.getClusterName(clusterName);
            if (!clusterName.empty()) {
                ss << clusterName << "/";
            }
            string processorTaskId = identifier.getTaskId();
            if (processorTaskId != "0" && !processorTaskId.empty()) {
                ss << processorTaskId << "/";
            }
        }
    }

    ss << setw(5) << setfill('0') << pid.range().from() << "_";
    ss << setw(5) << setfill('0') << pid.range().to();
    return ss.str();
};

string PathDefine::getCheckpointFilePath(const string& zkRoot, const PartitionId& pid)
{
    string fileName = "checkpoint";
    return fslib::util::FileUtil::joinFilePath(zkRoot, fileName);
}

string PathDefine::getIndexPartitionLockPath(const string& zkRoot, const PartitionId& pid)
{
    assert(pid.clusternames_size() == 1);

    stringstream ss;
    ss << "generation." << pid.buildid().appname() << "." << pid.buildid().datatable() << "."
       << pid.buildid().generationid() << "." << pid.clusternames(0) << "." << pid.range().from() << "."
       << pid.range().to() << ".__lock__";
    return fslib::util::FileUtil::joinFilePath(zkRoot, ss.str());
}

bool PathDefine::partitionIdFromHeartbeatPath(const string& path, PartitionId& pid)
{
    vector<string> strVec = StringUtil::split(path, "/");
    if (strVec.size() != 3) {
        return false;
    }
    if (strVec[2] != ZK_CURRENT_STATUS_FILE && strVec[2] != ZK_TARGET_STATUS_FILE) {
        return false;
    }
    pid.Clear();
    const string& generationStr = strVec[0];
    vector<string> genVec = StringUtil::split(generationStr, ".");
    if (genVec.size() != 3 && genVec[0] != "generation") {
        return false;
    }
    pid.mutable_buildid()->set_datatable(genVec[1]);
    uint32_t gid;
    if (!StringUtil::fromString<uint32_t>(genVec[2], gid)) {
        return false;
    }
    pid.mutable_buildid()->set_generationid(gid);

    const string& partitionStr = strVec[1];
    vector<string> partVec = StringUtil::split(partitionStr, ".");
    if (partVec.size() != 3 && partVec.size() != 4) {
        return false;
    }
    RoleType role = ProtoUtil::fromRoleString(partVec[0]);
    uint32_t id = 1;
    if (ROLE_PROCESSOR != role) {
        *pid.add_clusternames() = partVec[id++];
    }
    uint32_t from, to;
    if (!StringUtil::fromString<uint32_t>(partVec[id++], from)) {
        return false;
    }
    if (!StringUtil::fromString<uint32_t>(partVec[id], to)) {
        return false;
    }
    pid.mutable_range()->set_from(from);
    pid.mutable_range()->set_to(to);
    return true;
}

string PathDefine::parseZkHostFromZkPath(const string& zkPath)
{
    if (!StringUtil::startsWith(zkPath, ZFS_PREFIX)) {
        BS_LOG(ERROR, "zkPath[%s] is not invalid.", zkPath.c_str());
        return "";
    }
    string tmpStr = zkPath.substr(ZFS_PREFIX_LEN);
    return tmpStr.substr(0, tmpStr.find("/"));
}

string PathDefine::getClusterCheckpointFileName(const string& generationDir, const string& clusterName,
                                                const versionid_t versionId)
{
    string clusterCheckpointDir = getClusterCheckpointDir(generationDir, clusterName);
    string clusterCheckpointFile = fslib::util::FileUtil::joinFilePath(clusterCheckpointDir, CLUSTER_CHECKPOINT_PREFIX);
    return clusterCheckpointFile + "." + StringUtil::toString(versionId);
}

string PathDefine::getClusterCheckpointDir(const string& generationDir, const string& clusterName)
{
    string clusterZkDir = fslib::util::FileUtil::joinFilePath(generationDir, clusterName);
    return fslib::util::FileUtil::joinFilePath(clusterZkDir, ZK_GENERATION_CHECKPOINT_DIR);
}

string PathDefine::getReservedCheckpointFileName(const string& generationDir, const string& clusterName)
{
    string clusterZkDir = fslib::util::FileUtil::joinFilePath(generationDir, clusterName);
    string reservedCheckpointFile = fslib::util::FileUtil::joinFilePath(clusterZkDir, RESERVED_CHECKPOINT_FILENAME);
    return reservedCheckpointFile;
}

string PathDefine::getSnapshotCheckpointFileName(const string& generationDir, const string& clusterName)
{
    string clusterZkDir = fslib::util::FileUtil::joinFilePath(generationDir, clusterName);
    string snapshotCheckpointFile = fslib::util::FileUtil::joinFilePath(clusterZkDir, SNAPSHOT_CHECKPOINT_FILENAME);
    return snapshotCheckpointFile;
}

}} // namespace build_service::common
