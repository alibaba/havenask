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
#ifndef NAVI_NAVITESTSERVER_H
#define NAVI_NAVITESTSERVER_H

#include "navi/common.h"
#include "navi/engine/Navi.h"
#include "navi/engine/ResourceMap.h"
#include <arpc/ANetRPCServer.h>
#include "aios/network/http_arpc/HTTPRPCServer.h"
#include <multi_call/config/MultiCallConfig.h>

namespace navi {

class CreatorManager;

struct TestBizConf : public autil::legacy::Jsonizable {
public:
    TestBizConf();
    bool addPartId(NaviPartId partId);
    NaviPartId getPartCount() const;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) override;
public:
    std::string bizConfig;
    NaviPartId partCount;
    std::set<NaviPartId> partIdSet;
};

class NaviTestServer
{
public:
    NaviTestServer(const std::string &name,
                   const std::string &loader,
                   const std::string &configPath,
                   const ResourceMapPtr &rootResourceMap);
    ~NaviTestServer();
public:
    bool addBiz(const std::string &bizName,
                NaviPartId partCount,
                NaviPartId partId,
                const std::string &bizConfig);
    void addSubConfig(multi_call::LocalConfig &localSubConfig);
    bool createDepends();
    bool start(const std::string &gigConfigStr,
               const std::shared_ptr<CreatorManager> &creatorManager);
    std::shared_ptr<CreatorManager> getCreatorManager() const;
    NaviPtr getNavi() const;
    bool isSame(const std::string &serverName,
                const std::string &loader,
                const std::string &configPath,
                const ResourceMapPtr &rootResourceMap);
private:
    bool createGigServer();
    bool startNavi(const std::string &gigConfigStr,
                   const std::shared_ptr<CreatorManager> &creatorManager);
    void getMetaInfo(const std::string &gigConfigStr,
                     autil::legacy::json::JsonMap &metaMap);
    std::string getLoadParam(const std::string &gigConfigStr);
    void loop();
private:
    std::string _name;
    std::string _loader;
    std::string _configPath;
    ResourceMapPtr _rootResourceMap;
    std::map<std::string, TestBizConf> _bizMap;
    uint64_t _grpcPort;
    uint64_t _arpcPort;
    uint64_t _httpPort;
    std::shared_ptr<multi_call::GigRpcServer> _gigServer;
    NaviPtr _navi;
    std::vector<multi_call::ServerBizTopoInfo> _topoInfoVec;
private:
    AUTIL_LOG_DECLARE();
};

}

#endif //NAVI_NAVITESTSERVER_H
