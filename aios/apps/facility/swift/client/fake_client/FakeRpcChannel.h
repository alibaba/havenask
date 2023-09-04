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

#include "arpc/ANetRPCChannel.h"
#include "arpc/CommonMacros.h"
#include "autil/Log.h"
#include "swift/common/Common.h"

namespace anet {
class Connection;
} // namespace anet
namespace autil {
class ThreadPool;
} // namespace autil

namespace swift {
namespace client {

class FakeRpcChannel : public arpc::ANetRPCChannel {
public:
    FakeRpcChannel(autil::ThreadPool *threadPool, anet::Connection *pConnection);
    ~FakeRpcChannel();

private:
    FakeRpcChannel(const FakeRpcChannel &);
    FakeRpcChannel &operator=(const FakeRpcChannel &);

public:
    virtual void CallMethod(const RPCMethodDescriptor *method,
                            RPCController *controller,
                            const RPCMessage *request,
                            RPCMessage *response,
                            RPCClosure *done);

private:
    void asyncCall();

private:
    RPCClosure *_done;
    autil::ThreadPool *_threadPool;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(FakeRpcChannel);
} // namespace client
} // namespace swift
