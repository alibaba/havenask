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
#include "navi/test_cluster/NaviTestCluster.h"
#include "autil/legacy/jsonizable.h"

namespace navi {

NaviTestCluster::NaviTestCluster() {}

NaviTestCluster::~NaviTestCluster() {
    for (const auto &pair : _servers) {
        delete pair.second;
    }
}

bool NaviTestCluster::addBiz(const std::string &serverName,
                             const std::string &bizName,
                             NaviPartId partCount,
                             NaviPartId partId,
                             const std::string &bizConfig)
{
    auto server = getServer(serverName);
    if (!server) {
        return false;
    }
    _bizSet.insert(bizName);
    auto ret = server->addBiz(bizName, partCount, partId, bizConfig);
    if (!ret) {
        return ret;
    }
    std::cout << "server " << serverName << ", biz: " << bizName
              << ", partId: " << partId << ", partCount: " << partCount
              << std::endl;
    return ret;
}

bool NaviTestCluster::addServer(const std::string &serverName,
                                const std::string &loader,
                                const std::string &configPath,
                                const ResourceMapPtr &rootResourceMap)
{
    auto it = _servers.find(serverName);
    if (_servers.end() != it) {
        return it->second->isSame(serverName, loader, configPath,
                                  rootResourceMap);
    }
    auto server = new NaviTestServer(serverName, loader, configPath,
            rootResourceMap);
    _servers[serverName] = server;
    return true;
}

NaviTestServer *NaviTestCluster::getServer(const std::string &serverName) const {
    auto it = _servers.find(serverName);
    if (_servers.end() != it) {
        return it->second;
    } else {
        return nullptr;
    }
}

NaviPtr NaviTestCluster::getNavi(const std::string &serverName) const {
    auto server = getServer(serverName);
    if (!server) {
        return nullptr;
    } else {
        return server->getNavi();
    }
}

bool NaviTestCluster::start() {
    if (_started) {
        return true;
    }
    for (const auto &pair : _servers) {
        auto server = pair.second;
        if (!server->createDepends()) {
            return false;
        }
    }
    auto gigConfigStr = getClusterGigConfigStr();
    std::shared_ptr<CreatorManager> creatorManager;
    for (const auto &pair : _servers) {
        auto server = pair.second;
        if (!server->start(gigConfigStr, creatorManager)) {
            return false;
        }
        if (!creatorManager) {
            creatorManager = server->getCreatorManager();
        }
    }
    _started = true;
    return true;
}

std::string NaviTestCluster::getClusterGigConfigStr() const {
    auto localSubConfig = getClusterGigConfig();
    return autil::legacy::FastToJsonString(localSubConfig.nodes);
}

multi_call::LocalConfig NaviTestCluster::getClusterGigConfig() const {
    multi_call::LocalConfig localSubConfig;
    for (const auto &pair : _servers) {
        auto server = pair.second;
        server->addSubConfig(localSubConfig);
    }
    localSubConfig.nodes.insert(localSubConfig.nodes.end(), _externalConfig.nodes.begin(), _externalConfig.nodes.end());
    return localSubConfig;
}

void NaviTestCluster::addGigConfig(const multi_call::LocalConfig &config) {
    _externalConfig.nodes.insert(_externalConfig.nodes.end(), config.nodes.begin(), config.nodes.end());
}

std::vector<std::string> NaviTestCluster::getBizList() const {
    std::vector<std::string> bizList;
    bizList.reserve(_bizSet.size());
    for (const auto &biz : _bizSet) {
        bizList.emplace_back(biz);
    }
    return bizList;
}

}
