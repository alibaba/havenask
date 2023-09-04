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
#ifndef ARPC_RPC_SERVER_ADAPTER_H
#define ARPC_RPC_SERVER_ADAPTER_H
#include <unordered_set>

#include "aios/network/anet/anet.h"
#include "aios/network/arpc/arpc/CommonMacros.h"
#include "aios/network/arpc/arpc/proto/rpc_extensions.pb.h"

ARPC_BEGIN_NAMESPACE(arpc);

class RPCServer;
class RPCServerWorkItem;
struct CodecContext;
class Tracer;

class RPCServerAdapter {
public:
    RPCServerAdapter(RPCServer *pRpcServer);

    virtual ~RPCServerAdapter();

public:
    int getClientConnectionNum() { return atomic_read(&_clientConnNum); }

protected:
    ErrorCode doPushWorkItem(RPCServerWorkItem *pWorkItem, CodecContext *pContext, Tracer *tracer);

private:
    friend class RPCServerAdapterTest;

protected:
    RPCServer *_pRpcServer;
    atomic_t _clientConnNum;
    pthread_rwlock_t _lock;
    bool _skipThreadPool{false};
};

ARPC_END_NAMESPACE(arpc);

#endif // ARPC_RPC_SERVER_ADAPTER_H
