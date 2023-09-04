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

#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>

#include "autil/Log.h"
#include "autil/ThreadPool.h"
#include "swift/broker/service/LongPollingRequestHandler.h"
#include "swift/broker/service/RequestChecker.h"
#include "swift/common/Common.h"
#include "swift/log/BrokerLogClosure.h"
#include "swift/protocol/Transport.pb.h"

namespace google {
namespace protobuf {
class Closure;
class RpcController;
} // namespace protobuf
} // namespace google
namespace swift {
namespace monitor {
class BrokerMetricsReporter;
} // namespace monitor

namespace protocol {
class ConsumptionRequest;
class MessageIdRequest;
class MessageIdResponse;
class MessageResponse;
class ProductionRequest;
class SetLoggerLevelRequest;
class SetLoggerLevelResponse;
} // namespace protocol
namespace service {
class TopicPartitionSupervisor;
} // namespace service
namespace config {
class AuthorizerInfo;
class BrokerConfig;
} // namespace config

namespace util {
class IpMapper;
template <typename REQUEST, typename RESPONSE>
class LogClosure;
} // namespace util

namespace auth {
class RequestAuthenticator;

typedef std::shared_ptr<RequestAuthenticator> RequestAuthenticatorPtr;
} // namespace auth
} // namespace swift

namespace swift {
namespace service {

class TransporterImpl : public swift::protocol::Transporter {
public:
    TransporterImpl(TopicPartitionSupervisor *supervisor,
                    monitor::BrokerMetricsReporter *workerMetricsReporter = nullptr);
    ~TransporterImpl();
    TransporterImpl(const TransporterImpl &) = delete;
    TransporterImpl &operator=(const TransporterImpl &) = delete;

public:
    bool init(const config::BrokerConfig *config);
    size_t getMessageQueueSize() const;
    size_t getReadThreadNum() const;
    size_t getHoldQueueSize() const;
    size_t getHoldTimeoutCount() const;
    size_t getClearPollingQueueSize() const;

    void stop();
    void setIpMapper(util::IpMapper *ipMapper) { _ipMapper = ipMapper; }
    virtual void getMessage(::google::protobuf::RpcController *controller,
                            const ::swift::protocol::ConsumptionRequest *request,
                            ::swift::protocol::MessageResponse *response,
                            ::google::protobuf::Closure *done);
    virtual void sendMessage(::google::protobuf::RpcController *controller,
                             const ::swift::protocol::ProductionRequest *request,
                             ::swift::protocol::MessageResponse *response,
                             ::google::protobuf::Closure *done);
    virtual void getMaxMessageId(::google::protobuf::RpcController *controller,
                                 const ::swift::protocol::MessageIdRequest *request,
                                 ::swift::protocol::MessageIdResponse *response,
                                 ::google::protobuf::Closure *done);
    virtual void getMinMessageIdByTime(::google::protobuf::RpcController *controller,
                                       const ::swift::protocol::MessageIdRequest *request,
                                       ::swift::protocol::MessageIdResponse *response,
                                       ::google::protobuf::Closure *done);
    virtual void setLoggerLevel(::google::protobuf::RpcController *controller,
                                const ::swift::protocol::SetLoggerLevelRequest *request,
                                ::swift::protocol::SetLoggerLevelResponse *response,
                                ::google::protobuf::Closure *done);
    void requestPoll(BrokerPartitionPtr brokerPartition,
                     uint64_t compressPayload = 0,
                     int64_t maxMsgId = -1,
                     int64_t doneTime = -1);

private:
    template <typename REQUEST, typename RESPONSE>
    util::LogClosure<REQUEST, RESPONSE> *rpcCommon(const REQUEST *request,
                                                   RESPONSE *response,
                                                   google::protobuf::Closure *done,
                                                   const std::string &methodName,
                                                   BrokerPartitionPtr &brokerPartition,
                                                   bool needWriteType);
    BrokerPartitionPtr getBrokerPartition(const std::string &topicName, uint32_t id);
    bool isStopped() { return _stopped; }

private:
    bool initRequestAuthenticator(const config::AuthorizerInfo authInfo,
                                  const std::string &zkRoot,
                                  int64_t maxTopicAclSyncIntervalUs);

public:
    volatile bool _stopped = false;
    RequestChecker _requestChecker;
    TopicPartitionSupervisor *_tpSupervisor = nullptr;
    monitor::BrokerMetricsReporter *_workerMetricsReporter = nullptr;
    util::IpMapper *_ipMapper = nullptr;
    autil::ThreadPoolPtr _readThreadPool;
    LongPollingRequestHandlerPtr _longPollingRequestHandler;
    uint32_t _logSampleCount = 100;
    bool _closeForceLog = false;
    bool _enableLongPolling = false;
    auth::RequestAuthenticatorPtr _requestAuthenticator;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(TransporterImpl);

} // namespace service
} // namespace swift
