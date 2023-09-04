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
#include "suez/sdk/PathDefine.h"

#include <cstddef>
#include <limits.h>
#include <unistd.h>

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "suez/sdk/PartitionId.h"

using namespace std;
using namespace autil;

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, PathDefine);

namespace suez {

const std::string PathDefine::RUNTIMEDATA = "runtimedata";
const std::string PathDefine::ZONE_CONFIG = "zone_config";
const std::string PathDefine::LOCAL = "LOCAL";

const std::string PathDefine::APP = "app";
const std::string PathDefine::BIZ = "biz";

std::string PathDefine::join(const string &path, const string &file) {
    if (path.empty()) {
        return file;
    }

    if (path[path.length() - 1] == '/') {
        return path + file;
    }

    return path + '/' + file;
}

string PathDefine::getCurrentPath() {
    string path;
    getCurrentPath(path);
    return path;
}

bool PathDefine::getCurrentPath(std::string &path) {
    char cwdPath[PATH_MAX];
    char *ret = getcwd(cwdPath, PATH_MAX);
    if (NULL == ret) {
        AUTIL_LOG(ERROR, "Failed to get current work directory");
        return false;
    }
    path = string(cwdPath);
    if ('/' != *(path.rbegin())) {
        path += "/";
    }
    return true;
}

string PathDefine::getLocalIndexRoot() {
    string cwd;
    if (!PathDefine::getCurrentPath(cwd)) {
        return string();
    }
    return normalizeDir(join(cwd, RUNTIMEDATA));
}

string PathDefine::getLocalZoneConfigRoot() {
    string cwd;
    if (!PathDefine::getCurrentPath(cwd)) {
        return string();
    }
    return normalizeDir(join(cwd, ZONE_CONFIG));
}

string PathDefine::getLocalConfigBaseDir(const string &dirName) {
    return normalizeDir(join(getLocalZoneConfigRoot(), dirName));
}

string PathDefine::getLocalTableConfigBaseDir() { return normalizeDir(getLocalConfigBaseDir("table")); }

string PathDefine::getLocalTableConfigRoot(const string &tableName) {
    return normalizeDir(join(getLocalTableConfigBaseDir(), tableName));
}

string PathDefine::getLocalConfigRoot(const string &bizName, const string &dirName) {
    return normalizeDir(join(getLocalConfigBaseDir(dirName), bizName));
}

string PathDefine::getAdminZkRoot(const string &zkPath) { return normalizeDir(join(zkPath, "admin")); }

string PathDefine::getIsNewStartPath(const string &zkPath) { return join(zkPath, "is_new_start"); }

string PathDefine::getAdminStatusPath(const string &zkPath) { return join(getAdminZkRoot(zkPath), "admin_status"); }

string PathDefine::getAdminSerializeFile(const string &zkPath) { return getPathFromZkPath(getAdminStatusPath(zkPath)); }

string PathDefine::getAdminCatalogPath(const string &zkPath) { return join(getAdminZkRoot(zkPath), "catalog"); }

// zkPath: zfs://127.0.0.1:2181/path
// copy from worker_framework
#define ZFS_PREFIX "zfs://"
#define ZFS_PREFIX_LEN 6

string PathDefine::getHostFromZkPath(const std::string &zkPath) {
    if (!autil::StringUtil::startsWith(zkPath, ZFS_PREFIX)) {
        AUTIL_LOG(ERROR, "zkPath[%s] is not invalid.", zkPath.c_str());
        return "";
    }
    std::string tmpStr = zkPath.substr(ZFS_PREFIX_LEN);
    return tmpStr.substr(0, tmpStr.find("/"));
}

string PathDefine::getPathFromZkPath(const std::string &zkPath) {
    if (!autil::StringUtil::startsWith(zkPath, ZFS_PREFIX)) {
        AUTIL_LOG(ERROR, "zkPath[%s] is not invalid.", zkPath.c_str());
        return "";
    }
    std::string tmpStr = zkPath.substr(ZFS_PREFIX_LEN);
    std::string::size_type pos = tmpStr.find("/");
    if (pos == std::string::npos) {
        return "";
    }
    return tmpStr.substr(pos);
}

string PathDefine::getVersionSuffix(const string &configPath) {
    size_t pos = configPath.rfind('/', configPath.length() - 2);
    if (pos == string::npos) {
        return string();
    }
    return configPath.substr(pos + 1);
}

string PathDefine::normalizeDir(const string &path) {
    if (path.empty()) {
        return path;
    }

    if (*path.rbegin() != '/') {
        return path + '/';
    }
    return path;
}

std::string PathDefine::getFsType(const std::string &path) {
    size_t pos = path.find("://");
    if (pos == std::string::npos) {
        return LOCAL;
    }
    return path.substr(0, pos);
}

std::string PathDefine::getTableConfigFileName(const std::string &tableName) {
    return "clusters/" + tableName + "_cluster.json";
}

std::string PathDefine::getLeaderElectionRoot(const std::string &zkRoot) { return join(zkRoot, "leader_election"); }

std::string PathDefine::getLeaderInfoRoot(const std::string &zkRoot) { return join(zkRoot, "leader_info"); }

std::string PathDefine::getVersionStateRoot(const std::string &zkRoot) { return join(zkRoot, "versions"); }

std::string
PathDefine::getTableConfigPath(const std::string &rootPath, const std::string &tableName, const std::string &partName) {
    // TODO: 后续加上config的管理 按照时间戳生成 先无脑删除重新上传覆盖
    return join(join(rootPath, join(tableName, "config")), partName);
}

std::string PathDefine::getIndexRoot(const std::string &rootPath, const std::string &tableName) {
    return join(rootPath, join(tableName, "index"));
}

std::string PathDefine::getSchemaPath(const std::string &configPath, const std::string &tableName) {
    return join(configPath, "schemas/" + tableName + "_schema.json");
}

std::string PathDefine::getDataTablePath(const std::string &configPath, const std::string &tableName) {
    return join(configPath, "data_tables/" + tableName + "_table.json");
}

std::string PathDefine::getTableClusterConfigPath(const std::string &configPath, const std::string &tableName) {
    return join(configPath, getTableConfigFileName(tableName));
}

std::string PathDefine::getBuildServiceConfigPath(const std::string &configPath) {
    return join(configPath, "build_app.json");
}

std::string PathDefine::getBizConfigPath(const std::string &configPath, const std::string &deploymentName) {
    return PathDefine::join(
        configPath,
        PathDefine::join(deploymentName, PathDefine::join("biz_config", autil::TimeUtility::currentTimeString())));
}

} // namespace suez
