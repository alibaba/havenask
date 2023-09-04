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
#include "swift/client/fake_client/FakeRpcChannel.h"

#include <arpc/ANetRPCChannel.h>
#include <arpc/CommonMacros.h>
#include <functional>
#include <stddef.h>
#include <unistd.h>

#include "autil/LambdaWorkItem.h"
#include "autil/ThreadPool.h"
#include "autil/legacy/exception.h"

namespace anet {
class Connection;
} // namespace anet

using namespace autil;
namespace swift {
namespace client {
AUTIL_LOG_SETUP(swift, FakeRpcChannel);

FakeRpcChannel::FakeRpcChannel(autil::ThreadPool *threadPool, anet::Connection *pConnection)
    : arpc::ANetRPCChannel(pConnection, NULL, true) {
    _done = NULL;
    _threadPool = threadPool;
}

FakeRpcChannel::~FakeRpcChannel() {}

void FakeRpcChannel::CallMethod(const RPCMethodDescriptor *method,
                                RPCController *controller,
                                const RPCMessage *request,
                                RPCMessage *response,
                                RPCClosure *done) {
    (void)method;
    (void)controller;
    (void)request;
    (void)response;
    _done = done;
    _threadPool->pushWorkItem(new LambdaWorkItem(std::bind(&FakeRpcChannel::asyncCall, this)));
}

void FakeRpcChannel::asyncCall() {
    usleep(100000);
    if (_done) {
        _done->Run();
    }
}
} // namespace client
} // namespace swift
