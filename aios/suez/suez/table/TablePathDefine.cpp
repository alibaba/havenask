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
#include "suez/table/TablePathDefine.h"

#include <iosfwd>

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "build_service/common_define.h"
#include "suez/sdk/PathDefine.h"
#include "suez/table/TableMeta.h"

using namespace std;
using namespace autil;

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, TablePathDefine);

namespace suez {

const string TablePathDefine::GENERATION_PREFIX = "generation_";
const string TablePathDefine::PARTITION_PREFIX = "partition_";
const string TablePathDefine::GENERATION_META_FILE = "generation_meta";

string TablePathDefine::getGenerationDir(const PartitionId &pid) {
    return GENERATION_PREFIX + StringUtil::toString(pid.tableId.fullVersion);
}

string TablePathDefine::getIndexGenerationPrefix(const PartitionId &pid) {
    return PathDefine::join(pid.tableId.tableName, getGenerationDir(pid));
}

string TablePathDefine::getIndexPartitionPrefix(const PartitionId &pid) {
    string genDir = getGenerationDir(pid);
    string partDir = PARTITION_PREFIX + StringUtil::toString(pid.from) + "_" + StringUtil::toString(pid.to);
    return PathDefine::join(pid.tableId.tableName, PathDefine::join(genDir, partDir));
}

string TablePathDefine::getGenerationMetaFilePath(const PartitionId &pid) {
    return PathDefine::join(getIndexGenerationPrefix(pid), GENERATION_META_FILE);
}

string TablePathDefine::getRealtimeInfoFilePath(const PartitionId &pid) {
    return PathDefine::join(getIndexGenerationPrefix(pid), build_service::REALTIME_INFO_FILE_NAME);
}

string TablePathDefine::constructIndexPath(const string &indexRoot, const PartitionId &pid) {
    return PathDefine::join(indexRoot, getIndexPartitionPrefix(pid));
}

string TablePathDefine::constructLocalDataPath(const string &name, int64_t version, const string &dirName) {
    return PathDefine::join(PathDefine::getLocalConfigRoot(name, dirName), StringUtil::toString(version)) + "/";
}

string TablePathDefine::constructLocalConfigPath(const string &tableName, const string &remoteConfigPath) {
    auto suffix = PathDefine::getVersionSuffix(remoteConfigPath);
    if (suffix.empty()) {
        return string();
    } else {
        return PathDefine::join(PathDefine::getLocalTableConfigRoot(tableName), suffix) + "/";
    }
}

string TablePathDefine::constructTempConfigPath(const string &tableName,
                                                uint32_t from,
                                                uint32_t to,
                                                const string &remoteConfigPath) {
    auto suffix = autil::StringUtil::toString(from) + "_" + autil::StringUtil::toString(to) + "_" +
                  PathDefine::getVersionSuffix(remoteConfigPath);
    if (suffix.empty()) {
        return string();
    } else {
        auto tempBaseDir = PathDefine::normalizeDir(PathDefine::getLocalConfigBaseDir("table_temp_config"));
        auto tempConfigRoot = PathDefine::normalizeDir(PathDefine::join(tempBaseDir, tableName));
        return PathDefine::join(tempConfigRoot, suffix) + "/";
    }
}

string TablePathDefine::constructPartitionVersionFileName(const PartitionId &pid) {
    return pid.getTableName() + "_" + StringUtil::toString(pid.getFullVersion()) + "_" +
           StringUtil::toString(pid.from) + "_" + StringUtil::toString(pid.to) + ".version";
}

string TablePathDefine::constructPartitionLeaderElectPath(const PartitionId &pid, bool ignoreVersion) {
    if (ignoreVersion) {
        return pid.getTableName() + "_" + StringUtil::toString(pid.from) + "_" + StringUtil::toString(pid.to);
    }
    return pid.getTableName() + "_" + StringUtil::toString(pid.getFullVersion()) + "_" +
           StringUtil::toString(pid.from) + "_" + StringUtil::toString(pid.to);
}

std::string TablePathDefine::constructVersionPublishFilePath(const std::string &indexRoot,
                                                             const PartitionId &pid,
                                                             IncVersion versionId) {
    auto partitionRoot = constructIndexPath(indexRoot, pid);
    return PathDefine::join(partitionRoot, getVersionPublishFileName(versionId));
}

std::string TablePathDefine::getVersionPublishFileName(IncVersion versionId) {
    return "version.publish." + StringUtil::toString(versionId);
}

std::string TablePathDefine::constructPartitionDir(const PartitionId &pid) {
    return pid.getTableName() + "/" + StringUtil::toString(pid.getFullVersion()) + "/" +
           StringUtil::toString(pid.from) + "_" + StringUtil::toString(pid.to);
}

} // namespace suez
