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
#include <assert.h>
#include <google/protobuf/arena.h>
#include <google/protobuf/descriptor.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdint.h>
#include <string.h>
#include <iosfwd>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "aios/network/arpc/arpc/util/Log.h"
#include "autil/LockFreeThreadPool.h"
#include "aios/network/arpc/arpc/PacketArg.h"
#include "aios/network/arpc/arpc/RPCServerAdapter.h"
#include "aios/network/arpc/arpc/UtilFun.h"
#include "aios/network/arpc/arpc/RPCServerWorkItem.h"
#include "aios/network/arpc/arpc/RPCServer.h"
#include "aios/network/arpc/arpc/CommonMacros.h"
#include "aios/network/arpc/arpc/MessageCodec.h"
#include "aios/network/arpc/arpc/Tracer.h"
#include "autil/WorkItem.h"
#include "autil/EnvUtil.h"

using namespace std;
using namespace anet;
ARPC_BEGIN_NAMESPACE(arpc);
ARPC_DECLARE_AND_SETUP_LOGGER(RPCServerClosure);

RPCServerAdapter::RPCServerAdapter(RPCServer *pRpcServer) {
    _pRpcServer = pRpcServer;
    atomic_set(&_clientConnNum, 0);
    pthread_rwlockattr_t attr;
    pthread_rwlockattr_init(&attr);
    pthread_rwlock_init(&_lock, &attr);
    // TODO: this config could be expose on rpc server
    _skipThreadPool = autil::EnvUtil::getEnv("ARPC_SKIP_THREAD_POOL", _skipThreadPool);
}

RPCServerAdapter::~RPCServerAdapter()
{
    pthread_rwlock_destroy(&_lock);
}

ErrorCode RPCServerAdapter::doPushWorkItem(RPCServerWorkItem *pWorkItem,
                                      CodecContext *pContext,
                                      Tracer *tracer) {
    if (__builtin_expect(!!(_pRpcServer->isStopped()), 1)) {
        delete pWorkItem;
        return ARPC_ERROR_SERVER_CLOSED;
    }
    if (pWorkItem == NULL) {
        ARPC_LOG(ERROR, "new RPCServerWorkItem return NULL, "
                 "%s:%s", pContext->rpcMethodDes->service()->name().c_str(),
                 pContext->rpcMethodDes->name().c_str());
        return ARPC_ERROR_NEW_NOTHROW;
    }

    pWorkItem->setArena(pContext->arena);

    if (_skipThreadPool) {
        pWorkItem->process();
        pWorkItem->destroy();
        return ARPC_ERROR_NONE;
    }

    //TODO: drop work item maybe unacceptable
    autil::ThreadPoolBasePtr threadPoolPtr =
        _pRpcServer->GetServiceThreadPool(pContext->rpcService);

    if (threadPoolPtr == NULL) {
        ARPC_LOG(ERROR, "get service thread pool failed, "
                 "%s:%s", pContext->rpcMethodDes->service()->name().c_str(),
                 pContext->rpcMethodDes->name().c_str());
        delete pWorkItem;
        return ARPC_ERROR_PUSH_WORKITEM;
    }

    tracer->SetServerQueueSize(threadPoolPtr->getItemCount());

    autil::ThreadPool::ERROR_TYPE code =
        threadPoolPtr->pushWorkItem((autil::WorkItem *)pWorkItem, false);

    if (code != autil::ThreadPool::ERROR_NONE) {
        ARPC_LOG(ERROR,
                 "drop work item with request"
                 "thread pool errorcode=%d "
                 "%s:%s"
                 "thread pool name: %s, queue size: %lu, thread num: %lu",
                 code,
                 pContext->rpcMethodDes->service()->name().c_str(),
                 pContext->rpcMethodDes->name().c_str(),
                 (threadPoolPtr->getName()).c_str(), threadPoolPtr->getQueueSize(), threadPoolPtr->getThreadNum())
        delete pWorkItem;
        return ARPC_ERROR_PUSH_WORKITEM;
    }

    return ARPC_ERROR_NONE;
}

ARPC_END_NAMESPACE(arpc);

