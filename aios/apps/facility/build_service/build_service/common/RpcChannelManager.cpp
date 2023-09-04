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
#include "build_service/common/RpcChannelManager.h"

#include "aios/network/arpc/arpc/CommonMacros.h"
#include "autil/StringUtil.h"
#include "autil/legacy/jsonizable.h"
#include "fslib/util/FileUtil.h"
#include "worker_framework/LeaderLocator.h"

namespace build_service::common {

AUTIL_LOG_SETUP(madrox.slave, RpcChannelManager);

namespace {

constexpr const char* ZFS_PREFIX = "zfs://";
const size_t ZFS_PREFIX_LEN = ::strlen(ZFS_PREFIX);

struct LeaderInfo : public autil::legacy::Jsonizable {
    std::string address;
    int port = -1;
    std::string httpAddress;
    int httpPort = -1;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("address", address, address);
        json.Jsonize("port", port, port);
        json.Jsonize("httpAddress", httpAddress, httpAddress);
        json.Jsonize("httpPort", httpPort, httpPort);
    }
};

} // namespace

bool RpcChannelManager::Init(const std::string& zkRoot, bool isJsonFormat)
{
    _isJsonFormat = isJsonFormat;
    auto zkHost = getHostFromZkPath(zkRoot);
    if (zkHost.empty()) {
        return false;
    }
    _zkPath = getPathFromZkPath(zkRoot);
    if (_zkPath.empty()) {
        return false;
    }
    _zkWrapper = std::make_unique<cm_basic::ZkWrapper>(zkHost, DEFAULT_ZK_TIMEOUT);
    _rpcChannelManager = std::make_unique<arpc::ANetRPCChannelManager>();
    return _rpcChannelManager->StartPrivateTransport();
}

RpcChannelManager::~RpcChannelManager()
{
    if (_rpcChannelManager) {
        [[maybe_unused]] auto r = _rpcChannelManager->StopPrivateTransport();
        assert(r);
    }
}

std::string RpcChannelManager::convertToSpec(const std::string& addr)
{
    if (autil::StringUtil::startsWith(addr, "tcp:") || autil::StringUtil::startsWith(addr, "udp:") ||
        autil::StringUtil::startsWith(addr, "http:")) {
        return addr;
    } else {
        return "tcp:" + addr;
    }
}

std::shared_ptr<::google::protobuf::RpcChannel> RpcChannelManager::getRpcChannel()
{
    if (!_channel || ChannelBroken(_channel)) {
        auto spec = convertToSpec(getAdminTcpAddress());
        _channel = createRpcChannel(spec);
    }
    return _channel;
}

std::shared_ptr<::google::protobuf::RpcChannel> RpcChannelManager::createRpcChannel(const std::string& spec)
{
    bool block = false;
    size_t queueSize = 50ul;
    int timeout = 5000;
    bool autoReconnect = false;
    std::shared_ptr<::google::protobuf::RpcChannel> rpcChannel(
        _rpcChannelManager->OpenChannel(spec, block, queueSize, timeout, autoReconnect));
    if (!rpcChannel) {
        BS_LOG(ERROR, "open channel on [%s] failed", spec.c_str());
        return nullptr;
    }
    return rpcChannel;
}

std::string RpcChannelManager::getAdminTcpAddress()
{
    assert(_zkWrapper);
    if (_zkWrapper->isBad()) {
        _zkWrapper->close();
        _zkWrapper->open();
    }
    std::string content;
    std::string leaderPath = fslib::util::FileUtil::joinFilePath(_zkPath, "admin/LeaderElection");
    if (!_isJsonFormat) {
        leaderPath = fslib::util::FileUtil::joinFilePath(leaderPath, "leader_election0000000000");
    }
    if (_zkWrapper->getData(leaderPath, content) != ZOK) {
        BS_LOG(ERROR, "get leader info from [%s] failed", leaderPath.c_str());
        return std::string();
    }
    if (_isJsonFormat) {
        LeaderInfo leaderInfo;
        try {
            autil::legacy::FromJsonString(leaderInfo, content);
        } catch (const std::exception& e) {
            BS_LOG(ERROR, "from json string failed: content[%s], exception[%s]", content.c_str(), e.what());
            return std::string();
        }
        return leaderInfo.address;
    } else {
        const auto& addresses = autil::StringUtil::split(content, '\n');
        if (!addresses.empty()) {
            return addresses[0];
        } else {
            return std::string();
        }
    }
}

std::string RpcChannelManager::getHostFromZkPath(const std::string& zkPath)
{
    if (!autil::StringUtil::startsWith(zkPath, ZFS_PREFIX)) {
        BS_LOG(ERROR, "zkPath[%s] is not invalid.", zkPath.c_str());
        return "";
    }
    std::string tmpStr = zkPath.substr(ZFS_PREFIX_LEN);
    return tmpStr.substr(0, tmpStr.find("/"));
}

std::string RpcChannelManager::getPathFromZkPath(const std::string& zkPath)
{
    if (!autil::StringUtil::startsWith(zkPath, ZFS_PREFIX)) {
        BS_LOG(ERROR, "zkPath[%s] is not invalid.", zkPath.c_str());
        return "";
    }
    std::string tmpStr = zkPath.substr(ZFS_PREFIX_LEN);
    auto pos = tmpStr.find("/");
    if (pos == std::string::npos) {
        return "";
    }
    return tmpStr.substr(pos);
}

} // namespace build_service::common
