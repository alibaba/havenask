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
#ifndef ISEARCH_MULTI_CALL_CONNECTIONFACTORY_H
#define ISEARCH_MULTI_CALL_CONNECTIONFACTORY_H

#include "aios/network/anet/transport.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/grpc/client/GrpcClientWorker.h"
#include "aios/network/gig/multi_call/service/ArpcConnection.h"
#include "aios/network/gig/multi_call/service/GrpcConnection.h"
#include "aios/network/gig/multi_call/service/GrpcStreamConnection.h"
#include "aios/network/gig/multi_call/service/HttpConnection.h"
#include "aios/network/gig/multi_call/service/TcpConnection.h"

namespace grpc {
class ChannelCredentials;
class ChannelArguments;
} // namespace grpc

namespace multi_call {

struct GrpcChannelInitParams {
    int64_t keepAliveInterval = 0;
    int64_t keepAliveTimeout = 0;
};

class ConnectionFactory
{
public:
    ConnectionFactory(size_t queueSize) : _queueSize(queueSize) {
    }
    virtual ~ConnectionFactory() {
    }

private:
    ConnectionFactory(const ConnectionFactory &);
    ConnectionFactory &operator=(const ConnectionFactory &);

public:
    virtual ConnectionPtr createConnection(const std::string &spec) = 0;

protected:
    size_t _queueSize;

protected:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(ConnectionFactory);

template <typename ConnectionType>
class AnetConnectionFactory : public ConnectionFactory
{
public:
    AnetConnectionFactory(anet::Transport *transport, size_t queueSize)
        : ConnectionFactory(queueSize)
        , _transport(transport) {
    }
    ~AnetConnectionFactory() {
    }
    ConnectionPtr createConnection(const std::string &spec) override {
        return ConnectionPtr(new ConnectionType(_transport, spec, _queueSize));
    }

private:
    anet::Transport *_transport;
};

class ArpcConnectionFactory : public ConnectionFactory
{
public:
    ArpcConnectionFactory(const ANetRPCChannelManagerPtr &channelManager, size_t queueSize)
        : ConnectionFactory(queueSize)
        , _rpcChannelManager(channelManager) {
    }
    ~ArpcConnectionFactory() {
    }
    ConnectionPtr createConnection(const std::string &spec) override {
        return ConnectionPtr(new ArpcConnection(_rpcChannelManager, spec, _queueSize));
    }

private:
    ANetRPCChannelManagerPtr _rpcChannelManager;
};

class GrpcConnectionFactory : public ConnectionFactory
{
public:
    GrpcConnectionFactory(const GrpcClientWorkerPtr &worker, const SecureConfig &secureConfig)
        : ConnectionFactory(0)
        , _worker(worker)
        , _secureConfig(secureConfig) {
    }
    ~GrpcConnectionFactory() {
    }
    void initChannelArgs(GrpcChannelInitParams params = {});
    ConnectionPtr createConnection(const std::string &spec) override {
        return ConnectionPtr(new GrpcConnection(_worker, spec, _channelCredentials, _channelArgs));
    }
    // virtual for ut
    virtual grpc::SslCredentialsOptions getSslOptions() const;

protected:
    GrpcClientWorkerPtr _worker;
    SecureConfig _secureConfig;
    std::shared_ptr<grpc::ChannelCredentials> _channelCredentials;
    std::shared_ptr<grpc::ChannelArguments> _channelArgs;
};

class GrpcStreamConnectionFactory : public GrpcConnectionFactory
{
public:
    GrpcStreamConnectionFactory(const GrpcClientWorkerPtr &worker, const SecureConfig &secureConfig)
        : GrpcConnectionFactory(worker, secureConfig)
        , _objectPool(std::make_shared<ObjectPool>()) {
    }
    ~GrpcStreamConnectionFactory() {
    }
    ConnectionPtr createConnection(const std::string &spec) override {
        return ConnectionPtr(new GrpcStreamConnection(_worker, spec, _channelCredentials,
                                                      _channelArgs, _objectPool));
    }

private:
    ObjectPoolPtr _objectPool;
};

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_CONNECTIONFACTORY_H
