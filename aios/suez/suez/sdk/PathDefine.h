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

#include <string>

namespace suez {

struct PartitionId;

class PathDefine {
private:
    PathDefine();
    ~PathDefine();
    PathDefine(const PathDefine &);
    PathDefine &operator=(const PathDefine &);

public:
    static std::string join(const std::string &path, const std::string &file);
    static bool getCurrentPath(std::string &path);
    static std::string getCurrentPath();
    static std::string getLocalIndexRoot();
    static std::string getLocalZoneConfigRoot();
    static std::string getLocalConfigBaseDir(const std::string &dirName);
    static std::string getLocalTableConfigBaseDir();
    static std::string getLocalTableConfigRoot(const std::string &tableName);
    static std::string getLocalConfigRoot(const std::string &bizName, const std::string &dirName);
    static std::string getAdminZkRoot(const std::string &zkPath);
    static std::string getIsNewStartPath(const std::string &zkPath);
    static std::string getAdminStatusPath(const std::string &zkPath);
    static std::string getAdminSerializeFile(const std::string &zkPath);
    static std::string getAdminCatalogPath(const std::string &zkPath);

    static std::string getHostFromZkPath(const std::string &zkPath);
    static std::string getPathFromZkPath(const std::string &zkPath);
    static std::string getVersionSuffix(const std::string &configPath);

    static std::string normalizeDir(const std::string &path);

    static std::string getFsType(const std::string &path);

    static std::string getTableConfigFileName(const std::string &tableName);

    static std::string getLeaderElectionRoot(const std::string &zkRoot);
    static std::string getLeaderInfoRoot(const std::string &zkRoot);
    static std::string getVersionStateRoot(const std::string &zkRoot);

    static const std::string RUNTIMEDATA;
    static const std::string ZONE_CONFIG;
    static const std::string LOCAL;

    static const std::string APP;
    static const std::string BIZ;
};

} // namespace suez
