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

#include <assert.h>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>

#include "arpc/ANetRPCController.h"
#include "arpc/proto/rpc_extensions.pb.h"
#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "swift/client/Notifier.h"
#include "swift/client/SwiftTransportClient.h"
#include "swift/client/SwiftTransportClientCreator.h"
#include "swift/client/TransportClosure.h"
#include "swift/common/Common.h"
#include "swift/config/ClientAuthorizerInfo.h"
#include "swift/monitor/ClientMetricsReporter.h"
#include "swift/network/SwiftAdminAdapter.h"
#include "swift/network/SwiftRpcChannelManager.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"

namespace swift {
namespace client {

template <TransportRequestType EnumType>
class SwiftTransportAdapter {
public:
    typedef TransportClosureTyped<EnumType> ClosureType;
    typedef std::shared_ptr<ClosureType> ClosureTypePtr;
    typedef typename ClosureType::RequestType RequestType;
    typedef typename ClosureType::ResponseType ResponseType;

public:
    SwiftTransportAdapter(const network::SwiftAdminAdapterPtr &adminAdapter,
                          const network::SwiftRpcChannelManagerPtr &channelManager,
                          const std::string &topicName,
                          uint32_t partitionId,
                          Notifier *notifier = NULL,
                          int64_t retryTimeInterval = 500 * 1000);
    virtual ~SwiftTransportAdapter();

private:
    SwiftTransportAdapter(const SwiftTransportAdapter &);
    SwiftTransportAdapter &operator=(const SwiftTransportAdapter &);

public:
    // sync call
    protocol::ErrorCode
    postRequest(RequestType *request, ResponseType *response, int64_t timeout = common::DEFAULT_POST_TIMEOUT);
    protocol::ErrorCode postRequest(RequestType *request,
                                    int64_t timeout = common::DEFAULT_POST_TIMEOUT,
                                    monitor::ClientMetricsCollector *collector = nullptr);
    protocol::ErrorCode stealResponse(ResponseType *&response);

    bool waitLastRequestDone(int64_t timeout = -1);
    bool isLastRequestDone() const;
    bool isLastRequestHandled() const;
    int64_t getLastTransportClosureDoneTime() const;
    protocol::ErrorCode ignoreLastResponse() {
        waitLastRequestDone();
        return handleLastTransportClosure();
    }
    virtual int64_t getRetryInterval(int64_t currentTime) {
        if (currentTime >= _retryTime) {
            return 0;
        } else {
            return _retryTime - currentTime;
        }
    }
    void updateRetryTime(const protocol::ErrorCode ec, int64_t tryTime) {
        if (protocol::ERROR_BROKER_SOME_MESSAGE_LOST != ec && protocol::ERROR_NONE != ec) {
            _retryTime = tryTime + _retryTimeInterval;
        }
    }
    protocol::BrokerVersionInfo getBrokerVersionInfo() const { return _versionInfo; }
    void setIdStr(const std::string &idStr) { _idStr = idStr; }
    void setAuthInfo(const protocol::AuthenticationInfo &authInfo) { _authInfo = authInfo; }
    std::string getUsername() const { return _username; }

public: // for test
    void setSwiftTransportClient(SwiftTransportClient *transportClient) {
        DELETE_AND_SET_NULL(_transportClient);
        _transportClient = transportClient;
    }

protected:
    virtual protocol::ErrorCode createTransportClient();

private:
    bool needResetTransportClient(protocol::ErrorCode ec) const;
    protocol::ErrorCode prepareForPostRequest(monitor::ClientMetricsCollector *collector);
    protocol::ErrorCode handleLastTransportClosure();

protected:
    network::SwiftAdminAdapterPtr _adminAdapter;
    network::SwiftRpcChannelManagerPtr _channelManager;
    const std::string _topicName;
    uint32_t _partitionId;
    uint32_t _partitionCount;
    ClosureTypePtr _closure;
    bool _lastTransportClosureHandled;
    int64_t _retryTime;
    int64_t _retryTimeInterval;

    SwiftTransportClient *_transportClient = nullptr;
    bool _ownNotifier;
    Notifier *_notifier = nullptr;
    protocol::BrokerVersionInfo _versionInfo;
    std::string _idStr;
    protocol::AuthenticationInfo _authInfo;
    std::string _username;

private:
    friend class SwiftTransportAdapterTest;
    friend class FakeClientHelper;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE_WITH_TYPENAME(swift, SwiftTransportAdapter, TransportRequestType, EnumType);
/////////////////////////////////////////////////////////////////////////////////

template <TransportRequestType EnumType>
SwiftTransportAdapter<EnumType>::SwiftTransportAdapter(const network::SwiftAdminAdapterPtr &adminAdapter,
                                                       const network::SwiftRpcChannelManagerPtr &channelManager,
                                                       const std::string &topicName,
                                                       uint32_t partitionId,
                                                       Notifier *notifier,
                                                       int64_t retryTimeInterval)
    : _adminAdapter(adminAdapter)
    , _channelManager(channelManager)
    , _topicName(topicName)
    , _partitionId(partitionId)
    , _partitionCount(0)
    , _lastTransportClosureHandled(true)
    , _retryTime(autil::TimeUtility::currentTime())
    , _retryTimeInterval(retryTimeInterval)
    , _transportClient(NULL) {
    if (NULL == notifier) {
        _notifier = new Notifier;
        _ownNotifier = true;
    } else {
        _notifier = notifier;
        _ownNotifier = false;
    }
    if (NULL != _adminAdapter) {
        _username = _adminAdapter->getAuthenticationConf().username;
    }
}

template <TransportRequestType EnumType>
SwiftTransportAdapter<EnumType>::~SwiftTransportAdapter() {
    DELETE_AND_SET_NULL(_transportClient);
    _notifier->setNeedNotify(true);
    if (_closure) {
        _closure->stop();
        _closure.reset();
    }
    if (_ownNotifier) {
        DELETE_AND_SET_NULL(_notifier);
    }
}

template <TransportRequestType EnumType>
protocol::ErrorCode
SwiftTransportAdapter<EnumType>::postRequest(RequestType *request, ResponseType *response, int64_t timeout) {
    auto ec = prepareForPostRequest(nullptr);
    if (ec != protocol::ERROR_NONE) {
        updateRetryTime(ec, autil::TimeUtility::currentTime());
        AUTIL_LOG(WARN,
                  "[%s %d] Create transport client failed, error[%s]",
                  _topicName.c_str(),
                  _partitionId,
                  ErrorCode_Name(ec).c_str());
        return ec;
    }
    request->set_topicname(_topicName);
    request->set_partitionid(_partitionId);
    *request->mutable_authentication() = _authInfo;
    _transportClient->syncCall<EnumType>(request, response, timeout);
    ec = response->errorinfo().errcode();
    if (needResetTransportClient(ec)) {
        AUTIL_LOG(WARN,
                  "[%s %d] reset transport client for ec [%s]",
                  _topicName.c_str(),
                  _partitionId,
                  ErrorCode_Name(ec).c_str());
        if (_adminAdapter) {
            _adminAdapter->addErrorInfoToCacheTopic(_topicName, _partitionId, _transportClient->getAddress());
        }
        DELETE_AND_SET_NULL(_transportClient);
    }
    return ec;
}

template <TransportRequestType EnumType>
protocol::ErrorCode SwiftTransportAdapter<EnumType>::postRequest(RequestType *request,
                                                                 int64_t timeout,
                                                                 monitor::ClientMetricsCollector *collector) {
    protocol::ErrorCode ec = prepareForPostRequest(collector);
    if (ec != protocol::ERROR_NONE) {
        updateRetryTime(ec, autil::TimeUtility::currentTime());
        AUTIL_LOG(WARN,
                  "[%s %d] Create transport client failed, error[%s]",
                  _topicName.c_str(),
                  _partitionId,
                  ErrorCode_Name(ec).c_str());
        return ec;
    }
    assert(_transportClient);
    assert(!_closure);
    request->set_topicname(_topicName);
    request->set_partitionid(_partitionId);
    *request->mutable_authentication() = _authInfo;
    _closure = std::make_shared<ClosureType>(request, _notifier, timeout);
    if (_ownNotifier) {
        _notifier->setNeedNotify(true);
    }
    _lastTransportClosureHandled = false;
    _closure->prepare();
    _transportClient->asyncCall(_closure.get());
    return protocol::ERROR_NONE;
}

template <TransportRequestType EnumType>
protocol::ErrorCode SwiftTransportAdapter<EnumType>::stealResponse(ResponseType *&response) {
    protocol::ErrorCode ec = handleLastTransportClosure();
    response = _closure->stealResponse();
    _closure->log();
    return ec;
}

template <TransportRequestType EnumType>
bool SwiftTransportAdapter<EnumType>::waitLastRequestDone(int64_t timeout) {
    _notifier->setNeedNotify(true);
    if (timeout > 0) {
        _notifier->wait(timeout);
    } else {
        while (!isLastRequestDone()) {
            _notifier->wait(timeout);
        }
    }
    return isLastRequestDone();
}

template <TransportRequestType EnumType>
bool SwiftTransportAdapter<EnumType>::isLastRequestDone() const {
    if (_closure) {
        return _closure->isDone();
    }
    return true;
}

template <TransportRequestType EnumType>
bool SwiftTransportAdapter<EnumType>::isLastRequestHandled() const {

    return _lastTransportClosureHandled;
}

template <TransportRequestType EnumType>
int64_t SwiftTransportAdapter<EnumType>::getLastTransportClosureDoneTime() const {
    assert(_closure);
    return _closure->getDoneTime();
}

template <TransportRequestType EnumType>
protocol::ErrorCode SwiftTransportAdapter<EnumType>::createTransportClient() {
    assert(_adminAdapter.get());
    assert(!_transportClient);
    std::string brokerAddress;
    protocol::ErrorCode ec =
        _adminAdapter->getBrokerInfo(_topicName, _partitionId, brokerAddress, _versionInfo, _authInfo);
    if (ec != protocol::ERROR_NONE) {
        AUTIL_LOG(WARN,
                  "[%s %d] create transport client failed of get broker "
                  "address failed, error[%s]",
                  _topicName.c_str(),
                  _partitionId,
                  ErrorCode_Name(ec).c_str());
        return ec;
    }
    _transportClient = SwiftTransportClientCreator::createTransportClient(brokerAddress, _channelManager, _idStr);
    return protocol::ERROR_NONE;
}

template <TransportRequestType EnumType>
protocol::ErrorCode SwiftTransportAdapter<EnumType>::prepareForPostRequest(monitor::ClientMetricsCollector *collector) {
    assert(isLastRequestDone());
    if (collector && _closure) {
        collector->rpcLatency = _closure->getRpcLatency();
        collector->networkLatency = _closure->getNetworkLatency();
        if (EnumType == TRT_GETMESSAGE) {
            collector->decompressLatency = _closure->getDecompressLatency();
        }
    }
    _closure.reset();
    if (!_transportClient) {
        protocol::ErrorCode ec = createTransportClient();
        if (ec != protocol::ERROR_NONE) {
            AUTIL_LOG(WARN,
                      "[%s %d] create transport client error [%s]!",
                      _topicName.c_str(),
                      _partitionId,
                      ErrorCode_Name(ec).c_str());
            return ec;
        }
    }
    return protocol::ERROR_NONE;
}

template <TransportRequestType EnumType>
protocol::ErrorCode SwiftTransportAdapter<EnumType>::handleLastTransportClosure() {
    protocol::ErrorCode ec = protocol::ERROR_NONE;
    if (_lastTransportClosureHandled) {
        return ec;
    }
    if (_closure == nullptr) {
        _lastTransportClosureHandled = true;
        return ec;
    }
    arpc::ANetRPCController *controller = _closure->getController();
    if (controller->Failed()) {
        AUTIL_LOG(WARN,
                  "[%s %d] rpc call failed, address [%s], fail reason [%d %s]",
                  _topicName.c_str(),
                  _partitionId,
                  _transportClient->getAddress().c_str(),
                  controller->GetErrorCode(),
                  controller->ErrorText().c_str());
        if (arpc::ARPC_ERROR_PUSH_WORKITEM == controller->GetErrorCode()) {
            ec = protocol::ERROR_BROKER_PUSH_WORK_ITEM;
        } else {
            if (arpc::ARPC_ERROR_TIMEOUT == controller->GetErrorCode()) {
                if (_channelManager) {
                    _channelManager->channelTimeout(_transportClient->getAddress());
                }
            }
            ec = protocol::ERROR_CLIENT_ARPC_ERROR;
        }
    } else {
        ec = _closure->getResponseErrorCode();
    }
    if (needResetTransportClient(ec)) {
        AUTIL_LOG(WARN,
                  "[%s %d] reset transport client for ec [%s]",
                  _topicName.c_str(),
                  _partitionId,
                  ErrorCode_Name(ec).c_str());
        if (_adminAdapter) {
            _adminAdapter->addErrorInfoToCacheTopic(_topicName, _partitionId, _transportClient->getAddress());
        }
        DELETE_AND_SET_NULL(_transportClient);
    }
    updateRetryTime(ec, _closure->getDoneTime());
    _closure->setHandledTime(autil::TimeUtility::currentTime());
    _lastTransportClosureHandled = true;
    return ec;
}

template <TransportRequestType EnumType>
bool SwiftTransportAdapter<EnumType>::needResetTransportClient(protocol::ErrorCode ec) const {
    return ec == protocol::ERROR_CLIENT_ARPC_ERROR || ec == protocol::ERROR_BROKER_STOPPED ||
           ec == protocol::ERROR_BROKER_TOPIC_PARTITION_NOT_FOUND;
}

} // namespace client
} // namespace swift
