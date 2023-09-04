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
#include "leader_client/LeaderClient.h"

#include "arpc/PooledChannelManager.h"
#include "autil/StringTokenizer.h"
#include "worker_framework/LeaderLocator.h"
#include "worker_framework/PathUtil.h"

using namespace std;

namespace arpc {

LeaderClient::LeaderClient(cm_basic::ZkWrapper *zkWrapper)
    : _zkWrapper(zkWrapper)
    , _pooledChannelManager(make_unique<PooledChannelManager>())
    , _maxWorkerMapSize(5000)
    , _expireTime(5000) {
    _parseCallback = [](const string &content) -> Spec {
        auto address = worker_framework::LeaderLocator::getLine(content, 1);
        auto slotId = worker_framework::LeaderLocator::getLine(content, 3);
        autil::StringView cstr(address);
        vector<string> strs = autil::StringTokenizer::tokenize(cstr, ":");
        std::string ip;
        std::string port;
        // TODO(tianxiao) check size == 2
        if (strs.size() >= 1) {
            ip = strs[0];
        }
        if (strs.size() >= 2) {
            port = strs[1];
        }
        return Spec(ip, port, slotId);
    };
}

LeaderClient::~LeaderClient() {
    _pooledChannelManager.reset();
    _workerSpecMap.clear();
}

void LeaderClient::registerSpecParseCallback(function<Spec(const string &)> callback) {
    _parseCallback = std::move(callback);
}

bool LeaderClient::matchExpectIdentifier(const Spec &spec, const Identifier &expectedIdentifier) const {
    if (expectedIdentifier.empty()) {
        return true;
    }
    if (spec.ip == expectedIdentifier.ip && spec.slotId == expectedIdentifier.slotId) {
        return true;
    }
    return false;
}

shared_ptr<ANetRPCChannel> LeaderClient::getRpcChannel(const std::string &zkPath,
                                                       const Identifier &expectedIdentifier) {
    auto spec = getWorkerSpec(zkPath);
    if (!matchExpectIdentifier(spec, expectedIdentifier)) {
        spec.clear();
        clearWorkerSpecCache(zkPath);
    }
    if (!spec.empty()) {
        auto rpcChannel = _pooledChannelManager->getRpcChannel(getRpcAddress(spec));
        if (rpcChannel) {
            return rpcChannel;
        } else {
            clearWorkerSpecCache(zkPath);
        }
    }
    spec = getWorkerSpecFromZk(zkPath);
    if (spec.empty()) {
        // worker not start yet!
        return nullptr;
    }
    if (!matchExpectIdentifier(spec, expectedIdentifier)) {
        return nullptr;
    }
    auto rpcChannel = _pooledChannelManager->getRpcChannel(getRpcAddress(spec));
    if (!rpcChannel) {
        return nullptr;
    }
    updateWorkerSpec(zkPath, spec);
    return rpcChannel;
}

void LeaderClient::clearWorkerSpecCache(const string &zkPath) {
    unique_lock<mutex> lock(_mu);
    _workerSpecMap.erase(zkPath);
}

void LeaderClient::updateWorkerSpec(const string &zkPath, const Spec &spec) {
    unique_lock<mutex> lock(_mu);
    if (_workerSpecMap.size() > _maxWorkerMapSize) {
        _workerSpecMap.clear();
    }
    _workerSpecMap[zkPath] = spec;
}

LeaderClient::Spec LeaderClient::getWorkerSpec(const std::string &zkPath) {
    unique_lock<mutex> lock(_mu);
    auto it = _workerSpecMap.find(zkPath);
    if (it != _workerSpecMap.end()) {
        return it->second;
    }
    return {};
}

LeaderClient::Spec LeaderClient::getWorkerSpecFromZk(const string &zkPath) {
    worker_framework::LeaderLocator locator;
    if (!locator.init(_zkWrapper,
                      worker_framework::PathUtil::getLeaderInfoDir(zkPath),
                      worker_framework::PathUtil::LEADER_INFO_FILE_BASE)) {
        return {};
    }
    string addr = locator.doGetLeaderAddr();
    if (addr == "unknown") {
        return {};
    }
    Spec ret = _parseCallback(addr);
    return ret;
}

void LeaderClient::setExpireTime(const size_t expireTime) { _expireTime = expireTime; }

size_t LeaderClient::getExpireTime() const { return _expireTime; }
std::string LeaderClient::getRpcAddress(const Spec &spec) {
    return spec.ip + (spec.port.empty() ? "" : ":" + spec.port);
}

} // namespace arpc
