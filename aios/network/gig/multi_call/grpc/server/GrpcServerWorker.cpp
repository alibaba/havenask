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
#include "aios/network/gig/multi_call/grpc/server/GrpcServerWorker.h"

#include <grpc++/security/server_credentials.h>

#include "aios/network/gig/multi_call/grpc/server/GrpcServerStreamHandler.h"
#include "aios/network/gig/multi_call/rpc/GigRpcServer.h"
#include "aios/network/gig/multi_call/stream/GigStreamClosure.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, GrpcServerWorker);

GrpcServerWorker::GrpcServerWorker(GigRpcServer &owner)
    : _owner(owner)
    , _randomGenerator(0)
    , _callBackThreadPool(nullptr) {
}

GrpcServerWorker::~GrpcServerWorker() {
    stop();
    for (auto alarm : _shutdownAlarms) {
        delete alarm;
    }
    AUTIL_LOG(INFO, "grpc server worker stopped");
}

int32_t GrpcServerWorker::getPort() const {
    return _listenPort;
}

const GigAgentPtr &GrpcServerWorker::getAgent() const {
    return _owner.getAgent();
}

bool GrpcServerWorker::init(const GrpcServerDescription &desc) {
    const auto &port = desc.port;
    auto threadNum = desc.ioThreadNum;
    const auto &secureConfig = desc.secureConfig;

    if (0 == threadNum) {
        AUTIL_LOG(ERROR, "gig grpc ioThreadNum is 0");
        return false;
    }
    // TODO
    // _callBackThreadPool = new arpc::common::LockFreeThreadPool(
    //         threadNum, 1000, arpc::common::WorkItemQueueFactoryPtr(),
    //         "GigServerCB");
    // if (!_callBackThreadPool->start()) {
    //     AUTIL_LOG(ERROR, "start callback thread pool failed");
    //     return false;
    // }
    auto spec = "0.0.0.0:" + port;
    if (!secureConfig.pemRootCerts.empty()) {
        auto option = getSslOptions(secureConfig);
        _builder.AddListeningPort(spec, grpc::SslServerCredentials(option), &_listenPort);
    } else {
        _builder.AddListeningPort(spec, grpc::InsecureServerCredentials(), &_listenPort);
    }
    constexpr int32_t maxMsgSize = 1024 * 1024 * 1024;
    _builder.SetMaxReceiveMessageSize(maxMsgSize);
    _builder.SetMaxSendMessageSize(maxMsgSize);
    _builder.RegisterAsyncGenericService(&_genericService);
    if (threadNum <= 0) {
        AUTIL_LOG(ERROR, "invalid thread num %lu", threadNum);
        return false;
    }
    for (size_t i = 0; i < threadNum; i++) {
        ServerCompletionQueuePtr cq(_builder.AddCompletionQueue().release());
        ServerCompletionQueueStatusPtr cqs(new ServerCompletionQueueStatus(cq));
        _completionQueues.push_back(cqs);
    }
    _server = _builder.BuildAndStart();
    if (!_server) {
        AUTIL_LOG(ERROR, "start grpc server failed, port [%s], threadNum [%lu]", port.c_str(),
                  threadNum);
        return false;
    } else {
        for (size_t i = 0; i < _completionQueues.size(); i++) {
            _workThreads.push_back(autil::Thread::createThread(
                std::bind(&GrpcServerWorker::workLoop, this, i, _completionQueues[i]),
                "grpc_server_io"));
        }
    }
    _desc = desc;
    AUTIL_LOG(INFO, "start grpc server success, port [%s], threadNum [%lu]", port.c_str(),
              threadNum);
    return true;
}

grpc::SslServerCredentialsOptions
GrpcServerWorker::getSslOptions(const SecureConfig &secureConfig) const {
    grpc::SslServerCredentialsOptions option;
    option.pem_root_certs = secureConfig.pemRootCerts;
    grpc::SslServerCredentialsOptions::PemKeyCertPair pair;
    pair.private_key = secureConfig.pemPrivateKey;
    pair.cert_chain = secureConfig.pemCertChain;
    option.pem_key_cert_pairs.push_back(pair);
    option.client_certificate_request = GRPC_SSL_REQUEST_AND_REQUIRE_CLIENT_CERTIFICATE_AND_VERIFY;
    return option;
}

void GrpcServerWorker::stop() {
    AUTIL_LOG(DEBUG, "grpc server [%p] stop begin", this);
    if (!_server) {
        return;
    }
    if (_callBackThreadPool) {
        _callBackThreadPool->stop();
    }
    for (const auto &cqs : _completionQueues) {
        autil::ScopedReadWriteLock lock(cqs->enqueueLock, 'w');
        cqs->stopped = true;
    }
    AUTIL_LOG(DEBUG, "grpc server [%p] queue stopped", this);
    auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(100);
    _server->Shutdown(deadline);
    // _server->Shutdown();
    for (const auto &cqs : _completionQueues) {
        _shutdownAlarms.push_back(
            new ::grpc::Alarm(cqs->cq.get(), gpr_now(GPR_CLOCK_MONOTONIC), nullptr));
    }
    _completionQueues.clear();
    _workThreads.clear();
    DELETE_AND_SET_NULL(_callBackThreadPool);
    AUTIL_LOG(DEBUG, "grpc server [%p] stop end", this);
}

void GrpcServerWorker::workLoop(size_t idx, ServerCompletionQueueStatusPtr cqsPtr) {
    auto &cqs = *cqsPtr;
    auto &cq = *cqs.cq;
    AUTIL_LOG(INFO, "server work loop started, worker [%p] idx [%lu] cq [%p]", this, idx, &cq);
    {
        GrpcServerStreamHandlerPtr handler(new GrpcServerStreamHandler(this, cqsPtr));
        handler->setCallBackThreadPool(_callBackThreadPool);
        handler->init();
    }
    for (;;) {
        void *tag;
        bool ok = false;
        AUTIL_LOG(DEBUG, "[%p] grpc server loop begin", this);
        if (!cq.Next(&tag, &ok)) {
            break;
        }
        if (tag) {
            auto closure = static_cast<GigStreamClosureBase *>(tag);
            closure->run(ok);
        } else {
            // shutdown alarm
            autil::ScopedReadWriteLock lock(cqsPtr->enqueueLock, 'w');
            cqs.stopped = true;
            cq.Shutdown();
            AUTIL_LOG(INFO, "cq: %p shutdown", cqsPtr->cq.get());
        }
        AUTIL_LOG(DEBUG, "[%p] grpc server loop end", this);
    }
    AUTIL_LOG(INFO, "server work loop stopped, worker [%p] idx [%lu] cq [%p]", this, idx, &cq);
}

} // namespace multi_call
