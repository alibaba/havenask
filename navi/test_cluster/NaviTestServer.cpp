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

#include "navi/test_cluster/NaviTestServer.h"
#include "navi/util/CommonUtil.h"
#include <multi_call/rpc/GigRpcServer.h>

namespace navi {

AUTIL_LOG_SETUP(multi_call, NaviTestServer);

TestBizConf::TestBizConf()
    : partCount(INVALID_NAVI_PART_COUNT)
{
}

bool TestBizConf::addPartId(NaviPartId partId) {
    auto it = partIdSet.find(partId);
    if (partIdSet.end() != it) {
        return false;
    }
    partIdSet.insert(partId);
    return true;
}

NaviPartId TestBizConf::getPartCount() const {
    return partCount;
}

void TestBizConf::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("biz_config", bizConfig, bizConfig);
    json.Jsonize("part_count", partCount, partCount);
    json.Jsonize("part_ids", partIdSet, partIdSet);
}

NaviTestServer::NaviTestServer(const std::string &name,
                               const std::string &loader,
                               const std::string &configPath,
                               const ResourceMapPtr &rootResourceMap)
    : _name(name)
    , _loader(loader)
    , _configPath(configPath)
    , _rootResourceMap(rootResourceMap)
    , _grpcPort(0)
    , _arpcPort(0)
    , _httpPort(0)
{
}

NaviTestServer::~NaviTestServer() {
    if (_navi) {
        _navi->stop();
        _navi.reset();
    }
    _gigServer.reset();
}

bool NaviTestServer::addBiz(const std::string &bizName,
                            NaviPartId partCount,
                            NaviPartId partId,
                            const std::string &bizConfig)
{
    if (_navi) {
        std::cout << "add biz to started server " << _name << std::endl;
        return false;
    }
    auto &bizConf = _bizMap[bizName];
    auto prevPartCount = bizConf.partCount;
    if (INVALID_NAVI_PART_COUNT == prevPartCount) {
        bizConf.partCount = partCount;
    } else if (prevPartCount != partCount) {
        return false;
    }
    if (bizConf.bizConfig.empty()) {
        bizConf.bizConfig = bizConfig;
    } else if (bizConf.bizConfig != bizConfig) {
        return false;
    }
    return bizConf.addPartId(partId);
}

void NaviTestServer::addSubConfig(multi_call::LocalConfig &localSubConfig) {
    if (_grpcPort == 0) {
        std::cout << "addSubConfig failed, server not started: " << _name << std::endl;
    }
    assert(_grpcPort != 0);
    assert(_arpcPort != 0);
    assert(_httpPort != 0);
    for (const auto &pair : _bizMap) {
        const auto &bizName = pair.first;
        const auto &bizConf = pair.second;
        auto partCount = bizConf.getPartCount();
        for (auto partId : bizConf.partIdSet) {
            multi_call::LocalJsonize bizSub;
            bizSub._bizName = bizName;
            bizSub._partCnt = partCount;
            bizSub._partId = partId;
            bizSub._ip = "0.0.0.0";
            bizSub._tcpPort = _arpcPort;
            bizSub._httpPort = _httpPort;
            bizSub._grpcPort = _grpcPort;
            bizSub._rdmaPort = multi_call::INVALID_PORT;
            bizSub._version = 0;
            bizSub._supportHeartbeat = true;

            localSubConfig.nodes.push_back(bizSub);

            multi_call::ServerBizTopoInfo serverTopoInfo;
            serverTopoInfo.bizName = bizName;
            serverTopoInfo.partCount = partCount;
            serverTopoInfo.partId = partId;
            serverTopoInfo.version = 0;
            serverTopoInfo.protocolVersion = 0;
            serverTopoInfo.metas["meta_key1"] = "1";
            serverTopoInfo.metas["meta_key2"] = "2";
            serverTopoInfo.tags["tag1"] = 0;
            serverTopoInfo.tags["tag2"] = 0;
            serverTopoInfo.targetWeight = 30;
            _topoInfoVec.push_back(serverTopoInfo);
        }
    }
    loop();
}

void NaviTestServer::loop() {
    for (auto &topo : _topoInfoVec) {
        topo.metas["meta_key1"] =
            autil::StringUtil::toString((autil::StringUtil::fromString<int>(topo.metas["meta_key1"])) + 1);
    }
    std::vector<multi_call::SignatureTy> signatureVec;
    if (!_gigServer->publishGroup((multi_call::PublishGroupTy)this, _topoInfoVec, signatureVec)) {
        std::cout << "publish failed: " << _name << std::endl;
    }
}

bool NaviTestServer::start(const std::string &gigConfigStr,
                           const std::shared_ptr<CreatorManager> &creatorManager)
{
    if (_navi) {
        return true;
    }
    if (!startNavi(gigConfigStr, creatorManager)) {
        return false;
    }
    return true;
}

bool NaviTestServer::createDepends()
{
    if (_grpcPort != 0 || _arpcPort != 0 || _httpPort != 0) {
        std::cout << "duplicate create" << std::endl;
        return false;
    }
    if (!createGigServer()) {
        return false;
    }
    std::cout << _name << " started, arpc port: " << _arpcPort
              << ", grpc port: " << _grpcPort << ", http port: " << _httpPort
              << std::endl;
    return true;
}

bool NaviTestServer::startNavi(
        const std::string &gigConfigStr,
        const std::shared_ptr<CreatorManager> &creatorManager)
{
    NaviPtr navi(new Navi(creatorManager));
    if (!navi->init("", _gigServer.get())) {
        return false;
    }
    ResourceMap rootResourceMap;
    if (_rootResourceMap) {
        rootResourceMap.addResource(*_rootResourceMap);
    }
    const auto &loadParam = getLoadParam(gigConfigStr);
    if (!navi->update(_loader, _configPath, loadParam, rootResourceMap)) {
        return false;
    }
    auto instanceId = navi->getInstanceId();
    SessionId id;
    id.instance = instanceId;
    std::cout << "server " << _name
              << ", instance: " << CommonUtil::formatSessionId(id, instanceId)
              << std::endl;
    _navi = navi;
    return true;
}

void NaviTestServer::getMetaInfo(const std::string &gigConfigStr,
                                 autil::legacy::json::JsonMap &metaMap)
{
    metaMap["server_name"] = _name;
    metaMap["biz_list"] = autil::legacy::ToJson(_bizMap);
    metaMap["gig_sub_config"] = gigConfigStr;
    metaMap["gig_host"] = _name;
}

std::string NaviTestServer::getLoadParam(const std::string &gigConfigStr) {
    autil::legacy::json::JsonMap metaMap;
    getMetaInfo(gigConfigStr, metaMap);
    autil::legacy::json::JsonMap param;
    param["meta_info"] = metaMap;
    return autil::legacy::ToJsonString(param);
}

std::shared_ptr<CreatorManager> NaviTestServer::getCreatorManager() const {
    if (_navi) {
        return _navi->getCreatorManager();
    } else {
        return nullptr;
    }
}

NaviPtr NaviTestServer::getNavi() const {
    return _navi;
}

bool NaviTestServer::isSame(const std::string &serverName,
                            const std::string &loader,
                            const std::string &configPath,
                            const ResourceMapPtr &rootResourceMap)
{
    return _name == serverName
        && _loader == loader
        && _configPath == configPath
        && _rootResourceMap == rootResourceMap;
}

bool NaviTestServer::createGigServer() {
    multi_call::GigRpcServerPtr gigServer(new multi_call::GigRpcServer());
    multi_call::GigRpcServerConfig serverConfig;
    serverConfig.logPrefix = _name;
    if (!gigServer->init(serverConfig)) {
        return false;
    }
    {
        multi_call::ArpcServerDescription desc;
        desc.ioThreadNum = 1;
        desc.threadNum = 1;
        desc.queueSize = 100;
        desc.name = _name;
        desc.port = std::to_string(_arpcPort);
        if (!gigServer->startArpcServer(desc)) {
            return false;
        }
        _arpcPort = gigServer->getArpcServer()->GetListenPort();
    }
    {
        multi_call::HttpArpcServerDescription desc;
        desc.ioThreadNum = 1;
        desc.threadNum = 1;
        desc.queueSize = 100;
        desc.decodeUri = true;
        desc.haCompatible = true;
        desc.name = _name;
        desc.port = std::to_string(_httpPort);
        if (!gigServer->startHttpArpcServer(desc)) {
            return false;
        }
        _httpPort = gigServer->getHttpArpcServer()->getPort();
    }
    {
        multi_call::GrpcServerDescription desc;
        auto grpcPortStr = autil::StringUtil::toString(_grpcPort);
        desc.port = grpcPortStr;
        desc.ioThreadNum = 20;
        if (!gigServer->initGrpcServer(desc)) {
            std::cout << "init grpc server failed, port:" + grpcPortStr << std::endl;
            return false;
        }
    }
    gigServer->startAgent();
    _gigServer = gigServer;
    _grpcPort = gigServer->getGrpcPort();
    return true;
}

}
