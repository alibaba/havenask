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
#include "swift/broker/service/TransporterImpl.h"

#include <google/protobuf/stubs/callback.h>
#include <iosfwd>
#include <list>
#include <memory>
#include <type_traits>
#include <utility>

#include "autil/TimeUtility.h"
#include "swift/auth/RequestAuthenticator.h"
#include "swift/auth/TopicAclDataManager.h"
#include "swift/auth/TopicAclRequestHandler.h"
#include "swift/broker/service/BrokerPartition.h"
#include "swift/broker/service/LongPollingRequestHandler.h"
#include "swift/broker/service/ReadWorkItem.h"
#include "swift/broker/service/TopicPartitionSupervisor.h"
#include "swift/broker/storage/BrokerResponseError.h"
#include "swift/broker/storage/MessageGroup.h"
#include "swift/config/AuthorizerInfo.h"
#include "swift/config/BrokerConfig.h"
#include "swift/log/BrokerLogClosure.h"
#include "swift/log/LogClosure.h"
#include "swift/monitor/BrokerMetricsReporter.h"
#include "swift/monitor/TimeTrigger.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/util/Block.h"

namespace swift {
namespace service {
template <typename REQUEST>
struct RequestTraits;
} // namespace service
} // namespace swift

using namespace std;
using namespace autil;
using namespace swift::monitor;
using namespace swift::protocol;
using namespace swift::util;
using namespace swift::storage;
using namespace swift::config;

namespace swift {
namespace service {
AUTIL_LOG_SETUP(swift, TransporterImpl);

template <typename REQUEST>
struct RequestTraits {
    struct UnknownType {};
    typedef UnknownType ClosureType;
    static bool checkRequest(const REQUEST *request, RequestChecker &checker) { return false; }
};

#define IMPL_REQUEST_TRAITS(RequestType, ClosureTyped, methodName)                                                     \
    template <>                                                                                                        \
    struct RequestTraits<RequestType> {                                                                                \
        typedef ClosureTyped ClosureType;                                                                              \
        static bool checkRequest(const RequestType *request, RequestChecker &checker) {                                \
            return checker.methodName(request);                                                                        \
        }                                                                                                              \
    }

IMPL_REQUEST_TRAITS(protocol::ConsumptionRequest, swift::util::ConsumptionLogClosure, getMessage);
IMPL_REQUEST_TRAITS(protocol::ProductionRequest, swift::util::ProductionLogClosure, sendMessage);
IMPL_REQUEST_TRAITS(protocol::MessageIdRequest, swift::util::MessageIdLogClosure, getMaxMessageId);

#undef IMPL_REQUEST_2_CLOSURE

TransporterImpl::TransporterImpl(TopicPartitionSupervisor *tp, monitor::BrokerMetricsReporter *workerMetricsReporter)
    : _tpSupervisor(tp), _workerMetricsReporter(workerMetricsReporter) {}

TransporterImpl::~TransporterImpl() { _requestAuthenticator.reset(); }

bool TransporterImpl::init(const BrokerConfig *config) {
    _stopped = false;
    _readThreadPool.reset(new autil::ThreadPool(config->maxReadThreadNum, config->readQueueSize));
    if (!_readThreadPool->start("read_msg")) {
        AUTIL_LOG(ERROR, "start thread pool failed");
        return false;
    }

    _longPollingRequestHandler = make_shared<LongPollingRequestHandler>(this, _tpSupervisor);
    if (!_longPollingRequestHandler->init(config)) {
        AUTIL_LOG(ERROR, "no data request handler init failed.");
        return false;
    }
    _logSampleCount = config->getLogSampleCount();
    _closeForceLog = config->getCloseForceLog();
    if (config->getMaxGetMessageSizeKb() > 0) {
        _requestChecker.setMaxGetMessageSize(config->getMaxGetMessageSizeKb() * 1024);
    }
    if (!initRequestAuthenticator(config->authConf, config->zkRoot, config->maxTopicAclSyncIntervalUs)) {
        AUTIL_LOG(ERROR, "authenticator initialize failed.");
        return false;
    }

    return true;
}

bool TransporterImpl::initRequestAuthenticator(const AuthorizerInfo authInfo,
                                               const string &zkRoot,
                                               int64_t maxTopicAclSyncIntervalUs) {
    auth::TopicAclDataManagerPtr dataManager(new auth::TopicAclDataManager());
    if (!dataManager->init(zkRoot, true, maxTopicAclSyncIntervalUs)) {
        AUTIL_LOG(ERROR, "topic acl data manager init failed.");
        return false;
    }
    auth::TopicAclRequestHandlerPtr requestHandler(new auth::TopicAclRequestHandler(dataManager));
    _requestAuthenticator.reset(new auth::RequestAuthenticator(authInfo, requestHandler));
    return true;
}

size_t TransporterImpl::getMessageQueueSize() const { return _readThreadPool->getItemCount(); }
size_t TransporterImpl::getHoldQueueSize() const { return _longPollingRequestHandler->getHoldQueueSize(); }
size_t TransporterImpl::getHoldTimeoutCount() const { return _longPollingRequestHandler->getHoldTimeoutCount(); }
size_t TransporterImpl::getClearPollingQueueSize() const {
    return _longPollingRequestHandler->getClearPollingQueueSize();
}
size_t TransporterImpl::getReadThreadNum() const { return _readThreadPool->getActiveThreadNum(); }

void TransporterImpl::stop() {
    _stopped = true;
    // stop security mode backgroud thread first
    if (_tpSupervisor) {
        _tpSupervisor->stopSecurityModeThread();
    }

    if (_readThreadPool) {
        _readThreadPool->stop();
    }
    if (_longPollingRequestHandler) {
        _longPollingRequestHandler->stop();
    }
    _readThreadPool.reset();
    _longPollingRequestHandler.reset();

    // release the request held by broker partition before rpc destory
    if (_tpSupervisor) {
        _tpSupervisor->releaseLongPolling();
    }
}

BrokerPartitionPtr TransporterImpl::getBrokerPartition(const std::string &topicName, uint32_t id) {
    PartitionId partitionId;
    partitionId.set_topicname(topicName);
    partitionId.set_id(id);
    return _tpSupervisor->getBrokerPartition(partitionId);
}

void TransporterImpl::getMessage(::google::protobuf::RpcController *controller,
                                 const ::swift::protocol::ConsumptionRequest *request,
                                 ::swift::protocol::MessageResponse *response,
                                 ::google::protobuf::Closure *done) {
    (void)controller;
    BrokerPartitionPtr brokerPartition;
    using LogClosureTyped = typename RequestTraits<protocol::ConsumptionRequest>::ClosureType;
    std::shared_ptr<LogClosureTyped> closure(
        dynamic_cast<LogClosureTyped *>(rpcCommon(request, response, done, "getMessage", brokerPartition, false)));
    if (!closure || !brokerPartition) {
        return;
    }

    if (brokerPartition->sessionChanged(request)) {
        response->set_sessionid(brokerPartition->getSessionId());
        setBrokerResponseError(response->mutable_errorinfo(), ERROR_BROKER_SESSION_CHANGED);
        return;
    }
    ReadWorkItem *item = new ReadWorkItem(brokerPartition, closure, _longPollingRequestHandler.get(), this);
    ThreadPool::ERROR_TYPE error = _readThreadPool->pushWorkItem(item);
    if (ThreadPool::ERROR_NONE != error) {
        AUTIL_LOG(
            WARN, "add work item failed[%d (%s %d)]", error, request->topicname().c_str(), request->partitionid());
        delete item;
        setBrokerResponseError(response->mutable_errorinfo(), ERROR_BROKER_PUSH_WORK_ITEM);
    }
}

void TransporterImpl::sendMessage(::google::protobuf::RpcController *controller,
                                  const ::swift::protocol::ProductionRequest *request,
                                  ::swift::protocol::MessageResponse *response,
                                  ::google::protobuf::Closure *done) {
    (void)controller;
    using LogClosureTyped = typename RequestTraits<protocol::ProductionRequest>::ClosureType;
    BrokerPartitionPtr brokerPartition;
    std::unique_ptr<LogClosureTyped> closure(
        dynamic_cast<LogClosureTyped *>(rpcCommon(request, response, done, "sendMessage", brokerPartition, true)));
    if (!closure || !brokerPartition) {
        return;
    }

    if (brokerPartition->sessionChanged(request)) {
        response->set_sessionid(brokerPartition->getSessionId());
        setBrokerResponseError(response->mutable_errorinfo(), ERROR_BROKER_SESSION_CHANGED);
        return;
    }
    if (brokerPartition->sealed()) {
        setBrokerResponseError(response->mutable_errorinfo(), ERROR_TOPIC_SEALED);
        return;
    }
    _longPollingRequestHandler->registeCallBackBeforeSend(brokerPartition, closure.get());
    closure->setBrokerPartition(brokerPartition);
    brokerPartition->sendMessage(closure.release());
}

void TransporterImpl::requestPoll(BrokerPartitionPtr brokerPartition,
                                  uint64_t compressPayload,
                                  int64_t maxMsgId,
                                  int64_t doneTime) {
    if (isStopped()) {
        AUTIL_LOG(DEBUG, "service has stopped, drop request poll");
        return;
    }
    if (!brokerPartition) {
        AUTIL_LOG(WARN, "broker partition nullptr, drop request poll");
        return;
    }
    BrokerPartition::ReadRequestQueue reqs;
    brokerPartition->stealNeedActivePolling(reqs, compressPayload, maxMsgId);
    AUTIL_LOG(DEBUG, "request poll count [%lu] maxMsgId [%ld]", reqs.size(), maxMsgId);
    for (auto &req : reqs) {
        auto closurePtr = req.second;
        auto &timeTrigger = closurePtr->inQueueTimeTrigger;
        timeTrigger.endTrigger();
        auto inPollingTime = timeTrigger.getLatency();
        closurePtr->_longPollingLatencyOnce = inPollingTime;
        closurePtr->_longPollingLatencyAcc += inPollingTime;
        timeTrigger.beginTrigger();
        ReadWorkItem *item = new ReadWorkItem(brokerPartition, req.second, _longPollingRequestHandler.get(), this);
        item->setActiveMsgId(maxMsgId);
        ThreadPool::ERROR_TYPE error = _readThreadPool->pushWorkItem(item);
        if (ThreadPool::ERROR_NONE != error) {
            AUTIL_LOG(WARN, "add work item failed, error [%d], closure [%p]", error, closurePtr.get());
            delete item;
        } else {
            LongPollingMetricsCollector collector;
            collector.longPollingLatencyOnce = inPollingTime;
            if (doneTime > 0) {
                collector.longPollingLatencyActive = (autil::TimeUtility::currentTime() - doneTime) / 1000.0;
            }
            brokerPartition->reportLongPollingMetrics(collector);
            brokerPartition->reportLongPollingQps();
        }
    }
}

void TransporterImpl::getMaxMessageId(::google::protobuf::RpcController *controller,
                                      const ::swift::protocol::MessageIdRequest *request,
                                      ::swift::protocol::MessageIdResponse *response,
                                      ::google::protobuf::Closure *done) {
    (void)controller;
    using ClosureTyped = typename RequestTraits<protocol::MessageIdRequest>::ClosureType;
    BrokerPartitionPtr brokerPartition;
    std::unique_ptr<ClosureTyped> closure(
        dynamic_cast<ClosureTyped *>(rpcCommon(request, response, done, "getMaxMessageId", brokerPartition, false)));
    if (!closure || !brokerPartition) {
        return;
    }

    if (brokerPartition->sessionChanged(request)) {
        response->set_sessionid(brokerPartition->getSessionId());
        setBrokerResponseError(response->mutable_errorinfo(), ERROR_BROKER_SESSION_CHANGED);
        return;
    }
    ErrorCode ec = brokerPartition->getMaxMessageId(request, response);
    setBrokerResponseError(response->mutable_errorinfo(), ec);
}

void TransporterImpl::getMinMessageIdByTime(::google::protobuf::RpcController *controller,
                                            const ::swift::protocol::MessageIdRequest *request,
                                            ::swift::protocol::MessageIdResponse *response,
                                            ::google::protobuf::Closure *done) {
    (void)controller;
    using ClosureTyped = typename RequestTraits<protocol::MessageIdRequest>::ClosureType;
    BrokerPartitionPtr brokerPartition;
    std::unique_ptr<ClosureTyped> closure(dynamic_cast<ClosureTyped *>(
        rpcCommon(request, response, done, "getMinMessageIdByTime", brokerPartition, false)));
    if (!closure || !brokerPartition) {
        return;
    }
    if (!request->has_timestamp()) {
        setBrokerResponseError(response->mutable_errorinfo(), ERROR_BROKER_REQUEST_INVALID);
        return;
    }

    if (brokerPartition->sessionChanged(request)) {
        response->set_sessionid(brokerPartition->getSessionId());
        setBrokerResponseError(response->mutable_errorinfo(), ERROR_BROKER_SESSION_CHANGED);
        return;
    }
    ErrorCode ec = brokerPartition->getMinMessageIdByTime(request, response);
    setBrokerResponseError(response->mutable_errorinfo(), ec);
}

void TransporterImpl::setLoggerLevel(::google::protobuf::RpcController *controller,
                                     const ::swift::protocol::SetLoggerLevelRequest *request,
                                     ::swift::protocol::SetLoggerLevelResponse *response,
                                     ::google::protobuf::Closure *done) {
    (void)controller;
    (void)response;
    AUTIL_LOG(INFO, "set logger level, request:%s", request->ShortDebugString().c_str());
    alog::Logger *logger = alog::Logger::getLogger(request->logger().c_str());
    logger->setLevel(request->level());
    done->Run();
}

template <typename REQUEST, typename RESPONSE>
util::LogClosure<REQUEST, RESPONSE> *TransporterImpl::rpcCommon(const REQUEST *request,
                                                                RESPONSE *response,
                                                                google::protobuf::Closure *done,
                                                                const std::string &methodName,
                                                                BrokerPartitionPtr &brokerPartition,
                                                                bool needWrite) {
    brokerPartition.reset();
    using ClosureType = typename RequestTraits<REQUEST>::ClosureType;
    auto closure = std::make_unique<ClosureType>(request, response, done, methodName);
    closure->setBrokerMetricReporter(_workerMetricsReporter);
    closure->setIpMapper(_ipMapper);
    closure->setLogInterval(_logSampleCount);
    closure->setCloseForceLog(_closeForceLog);
    if (_stopped || _tpSupervisor->needRejectServing()) {
        setBrokerResponseError(response->mutable_errorinfo(), ERROR_BROKER_STOPPED);
        return nullptr;
    }
    if (!RequestTraits<REQUEST>::checkRequest(request, _requestChecker)) {
        setBrokerResponseError(response->mutable_errorinfo(), ERROR_BROKER_REQUEST_INVALID);
        return nullptr;
    }
    if (_requestAuthenticator != nullptr && !_requestAuthenticator->checkTopicAclInBroker(request, needWrite)) {
        setBrokerResponseError(response->mutable_errorinfo(),
                               ERROR_BROKER_INVALID_USER,
                               "topic acl check failed[" + request->topicname() + "," +
                                   request->authentication().username() + "," +
                                   request->authentication().accessauthinfo().accessid() + "]");
        return nullptr;
    }

    brokerPartition = getBrokerPartition(request->topicname(), request->partitionid());
    if (!brokerPartition) {
        AUTIL_LOG(WARN, "partition[%s:%d] not found", request->topicname().c_str(), request->partitionid());
        setBrokerResponseError(response->mutable_errorinfo(), ERROR_BROKER_TOPIC_PARTITION_NOT_FOUND);
        return nullptr;
    }
    int64_t topicVersion = brokerPartition->getPartitionId().version();
    if (topicVersion > 0) {
        response->set_topicversion(topicVersion);
    }
    bool canServeRequest = false;
    auto status = brokerPartition->getPartitionStatus();
    if constexpr (std::is_same<REQUEST, protocol::ConsumptionRequest>::value) {
        canServeRequest =
            status == PARTITION_STATUS_RUNNING || (status == PARTITION_STATUS_RECOVERING && request->has_selfversion());
    } else {
        canServeRequest = status == PARTITION_STATUS_RUNNING;
    }
    if (!canServeRequest) {
        setBrokerResponseError(response->mutable_errorinfo(), ERROR_BROKER_TOPIC_PARTITION_NOT_RUNNING);
        AUTIL_LOG(WARN,
                  "Partition[%s:%d] current not on running status",
                  request->topicname().c_str(),
                  request->partitionid());
        return nullptr;
    }
    return closure.release();
}

} // namespace service
} // namespace swift
