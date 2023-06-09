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
#ifndef NAVI_NAVITESTCLUSTER_H
#define NAVI_NAVITESTCLUSTER_H

#include "navi/common.h"
#include "navi/engine/Navi.h"
#include "navi/test_cluster/NaviTestServer.h"
#include <set>

namespace navi {

class NaviTestServer;

class NaviTestCluster
{
public:
    NaviTestCluster();
    ~NaviTestCluster();
private:
    NaviTestCluster(const NaviTestCluster &);
    NaviTestCluster &operator=(const NaviTestCluster &);
public:
    bool addServer(const std::string &serverName,
                   const std::string &loader,
                   const std::string &configPath,
                   const ResourceMapPtr &rootResourceMap);
    bool addBiz(const std::string &serverName,
                const std::string &bizName,
                NaviPartId partCount,
                NaviPartId partId,
                const std::string &bizConfig);
    bool start();
    NaviPtr getNavi(const std::string &serverName) const;
    std::vector<std::string> getBizList() const;
    multi_call::LocalConfig getClusterGigConfig() const;
    void addGigConfig(const multi_call::LocalConfig &config);
private:
    NaviTestServer *getServer(const std::string &serverName) const;
    std::string getClusterGigConfigStr() const;
private:
    std::unordered_map<std::string, NaviTestServer *> _servers;
    std::set<std::string> _bizSet;
    bool _started = false;
    multi_call::LocalConfig _externalConfig;
};

}

#endif //NAVI_NAVITESTCLUSTER_H
