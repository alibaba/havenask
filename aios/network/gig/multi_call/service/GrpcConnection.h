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
#ifndef ISEARCH_MULTI_CALL_GRPCCONNECTION_H
#define ISEARCH_MULTI_CALL_GRPCCONNECTION_H

#include "aios/network/gig/multi_call/config/MultiCallConfig.h"
#include "aios/network/gig/multi_call/grpc/client/GrpcClientWorker.h"
#include "aios/network/gig/multi_call/service/Connection.h"
#include "autil/Lock.h"

namespace grpc {
class ChannelCredentials;
class ChannelArguments;
} // namespace grpc

namespace multi_call {

typedef std::shared_ptr<grpc::ChannelInterface> GrpcChannelPtr;

class GrpcConnection : public Connection
{
public:
    GrpcConnection(const GrpcClientWorkerPtr &grpcWorker, const std::string &spec,
                   const std::shared_ptr<grpc::ChannelCredentials> &channelCredentials,
                   const std::shared_ptr<grpc::ChannelArguments> &channelArgs);
    GrpcConnection(const GrpcClientWorkerPtr &grpcWorker, const std::string &spec,
                   const std::shared_ptr<grpc::ChannelCredentials> &channelCredentials,
                   const std::shared_ptr<grpc::ChannelArguments> &channelArgs, ProtocolType type);
    ~GrpcConnection();

private:
    GrpcConnection(const GrpcConnection &);
    GrpcConnection &operator=(const GrpcConnection &);

public:
    void post(const RequestPtr &request, const CallBackPtr &callBack) override;
    GrpcChannelPtr getChannel();

protected:
    GrpcClientWorkerPtr _grpcWorker;
    autil::ReadWriteLock _channelLock;
    GrpcChannelPtr _channel;
    std::shared_ptr<grpc::ChannelCredentials> _channelCredentials;
    std::shared_ptr<grpc::ChannelArguments> _channelArgs;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(GrpcConnection);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GRPCCONNECTION_H
