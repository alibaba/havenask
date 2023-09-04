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
#include <stddef.h>
#include <stdint.h>
#include <string>

#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "swift/client/SwiftTransportAdapter.h"
#include "swift/client/TransportClosure.h"
#include "swift/client/fake_client/FakeSwiftTransportClient.h"
#include "swift/network/SwiftAdminAdapter.h"
#include "swift/protocol/ErrCode.pb.h"

namespace swift {
namespace client {

template <TransportRequestType EnumType>
class FakeSwiftTransportAdapter : public SwiftTransportAdapter<EnumType> {
public:
    FakeSwiftTransportAdapter(const std::string &topicName, uint32_t partitionId)
        : SwiftTransportAdapter<EnumType>(
              network::SwiftAdminAdapterPtr(), network::SwiftRpcChannelManagerPtr(), topicName, partitionId) {
        _fakeClient = new FakeSwiftTransportClient;
        _retryInterval = 0;
        _ec = protocol::ERROR_NONE;
        _ownClient = true;
        _func = NULL;
    }
    /** reset fake client before destruction*/
    FakeSwiftTransportAdapter(FakeSwiftTransportClient *fakeClient, const std::string &topicName, uint32_t partitionId)
        : SwiftTransportAdapter<EnumType>(
              network::SwiftAdminAdapterPtr(), network::SwiftRpcChannelManagerPtr(), topicName, partitionId) {
        _fakeClient = fakeClient;
        _retryInterval = 0;
        _ec = protocol::ERROR_NONE;
        _ownClient = false;
    }

    ~FakeSwiftTransportAdapter() {
        if (_ownClient) {
            resetFakeClient();
        }
    }

private:
    FakeSwiftTransportAdapter(const FakeSwiftTransportAdapter &);
    FakeSwiftTransportAdapter &operator=(const FakeSwiftTransportAdapter &);

public:
    FakeSwiftTransportClient *getFakeTransportClient() { return _fakeClient; }
    int64_t getRetryInterval(int64_t currentTime) {
        (void)currentTime;
        return _retryInterval;
    }
    void setRetryInterval(int64_t interval) { _retryInterval = interval; }
    void setErrorCode(protocol::ErrorCode ec) { _ec = ec; }
    void setCallBackFunc(const std::function<protocol::ErrorCode(int64_t)> &func) { _func = func; }

public:
    /** avoid double free same fake client */
    void resetFakeClient() {
        this->_transportClient = NULL;
        DELETE_AND_SET_NULL(_fakeClient);
    }

protected:
    /* override */
    protocol::ErrorCode createTransportClient() {
        if (_ec == protocol::ERROR_NONE) {
            this->_transportClient = _fakeClient;
        } else {
            delete _fakeClient;
            _fakeClient = NULL;
        }
        if (_func) {
            _ec = _func(0);
        }
        return _ec;
    }

private:
    FakeSwiftTransportClient *_fakeClient;
    int64_t _retryInterval;
    protocol::ErrorCode _ec;
    bool _ownClient;
    std::function<protocol::ErrorCode(int64_t)> _func;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace client
} // namespace swift
