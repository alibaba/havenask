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

#include <functional>
#include <memory>
#include <mutex>

#include "arpc/ANetRPCChannel.h"
#include "arpc/ANetRPCController.h"
#include "arpc/CommonMacros.h"
#include "autil/NoCopyable.h"

namespace cm_basic {
class ZkWrapper;
}

namespace arpc {
class PooledChannelManager;

template <typename Request, typename Response>
class OwnControllerClosure : public RPCClosure {
public:
    OwnControllerClosure(const std::function<void(OwnControllerClosure<Request, Response> *)> &handler)
        : handler(handler) {}
    ~OwnControllerClosure() = default;

    void Run() override {
        handler(this);
        delete this;
    }

public:
    std::function<void(OwnControllerClosure<Request, Response> *)> handler;
    Request request;
    Response response;
    arpc::ANetRPCController controller;
    std::string addr;
};

class LeaderClient : public autil::NoCopyable {
public:
    struct Identifier {
        Identifier() = default;
        Identifier(const std::string &ip, const std::string &slotId) : ip(ip), slotId(slotId) {
            assert(ip.empty() == slotId.empty());
        }
        bool empty() const { return ip.empty() && slotId.empty(); }
        std::string ip;
        std::string slotId;
    };
    // (ip, port, slotId)
    struct Spec {
        Spec() = default;
        Spec(const std::string &ip, const std::string &port, const std::string &slotId)
            : ip(ip), port(port), slotId(slotId) {}
        bool empty() const { return ip.empty() && port.empty() && slotId.empty(); }
        bool operator==(const Spec &other) const {
            return ip == other.ip && port == other.port && slotId == other.slotId;
        };
        void clear() {
            ip.clear();
            port.clear();
            slotId.clear();
        }
        std::string ip;
        std::string port;
        std::string slotId;
    };

public:
    LeaderClient(cm_basic::ZkWrapper *zkWrapper);
    ~LeaderClient();

public:
    // if expectSpec is not empty, will return nullptr is current leader identifier is not expectedIdentifier
    std::shared_ptr<ANetRPCChannel> getRpcChannel(const std::string &zkPath, const Identifier &expectedIdentifier);

public:
    // set getWorkerSpecFromZk
    void registerSpecParseCallback(std::function<Spec(const std::string &)> callback);
    Spec getWorkerSpecFromZk(const std::string &zkPath);
    void clearWorkerSpecCache(const std::string &zkPath);

public:
    void setExpireTime(const size_t timeout);
    size_t getExpireTime() const;
    static std::string getRpcAddress(const Spec &spec);

private:
    void updateWorkerSpec(const std::string &zkPath, const Spec &spec);
    Spec getWorkerSpec(const std::string &zkPath);
    bool matchExpectIdentifier(const Spec &spec, const Identifier &expectedIdentifier) const;

private:
    std::function<Spec(const std::string &)> _parseCallback;
    cm_basic::ZkWrapper *_zkWrapper;
    std::unique_ptr<PooledChannelManager> _pooledChannelManager;

    std::mutex _mu;
    // { zkpath : Spec }
    std::map<std::string, Spec> _workerSpecMap;
    size_t _maxWorkerMapSize;
    size_t _expireTime;
};

#define CALL_LEADER(client, StubType, Method, RequestType, ResponseType, requestValue, zkPath, cb, requiredSpec)       \
    CALL_LEADER_TIMEOUT(client,                                                                                        \
                        StubType,                                                                                      \
                        Method,                                                                                        \
                        RequestType,                                                                                   \
                        ResponseType,                                                                                  \
                        requestValue,                                                                                  \
                        zkPath,                                                                                        \
                        cb,                                                                                            \
                        client.getExpireTime(),                                                                        \
                        requiredSpec)

#define CALL_LEADER_TIMEOUT(                                                                                           \
    client, StubType, Method, RequestType, ResponseType, requestValue, zkPath, cb, timeout, requiredSpec)              \
    do {                                                                                                               \
        auto channel = client.getRpcChannel(zkPath, requiredSpec);                                                     \
        auto closure = new arpc::OwnControllerClosure<RequestType, ResponseType>(cb);                                  \
        closure->request = requestValue;                                                                               \
        closure->controller.SetExpireTime(timeout);                                                                    \
        if (!channel) {                                                                                                \
            closure->controller.SetFailed("null channel");                                                             \
            closure->Run();                                                                                            \
        } else {                                                                                                       \
            closure->addr = channel->getRemoteAddr();                                                                  \
            StubType stub(channel.get(), google::protobuf::Service::STUB_DOESNT_OWN_CHANNEL);                          \
            stub.Method(&(closure->controller), &(closure->request), &(closure->response), closure);                   \
        }                                                                                                              \
    } while (0)

} // namespace arpc
