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
#include "aios/network/gig/multi_call/grpc/client/GrpcClientWorker.h"

#include "aios/network/gig/multi_call/stream/GigStreamClosure.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, GrpcClientWorker);

GrpcClientWorker::GrpcClientWorker(size_t threadNum) {
    for (size_t i = 0; i < threadNum; i++) {
        CompletionQueueStatusPtr cqs(
            new CompletionQueueStatus(CompletionQueuePtr(new grpc::CompletionQueue())));
        _completionQueues.push_back(cqs);
        _workThreads.push_back(autil::Thread::createThread(
            std::bind(&GrpcClientWorker::workLoop, this, i, cqs), "grpc_client_io"));
    }
    AUTIL_LOG(INFO, "grpc client worker [%p] started, threads [%lu]", this, threadNum);
}

GrpcClientWorker::~GrpcClientWorker() {
    stop();
    for (auto alarm : _shutdownAlarms) {
        delete alarm;
    }
}

void GrpcClientWorker::stop() {
    for (const auto &cqs : _completionQueues) {
        autil::ScopedReadWriteLock lock(cqs->enqueueLock, 'w');
        cqs->stopped = true;
    }
    for (const auto &cqs : _completionQueues) {
        _shutdownAlarms.push_back(
            new ::grpc::Alarm(cqs->cq.get(), gpr_now(GPR_CLOCK_MONOTONIC), nullptr));
    }
    _completionQueues.clear();
    _workThreads.clear();
    AUTIL_LOG(INFO, "grpc client worker [%p] stopped", this);
}

const CompletionQueueStatusPtr &GrpcClientWorker::getCompletionQueue(size_t allocId) {
    size_t index = allocId % _completionQueues.size();
    return _completionQueues[index];
}

void GrpcClientWorker::workLoop(size_t idx, CompletionQueueStatusPtr cqsPtr) {
    auto &cq = *cqsPtr->cq;
    AUTIL_LOG(INFO, "client work loop started, worker [%p] idx [%lu] cq [%p]", this, idx, &cq);
    for (;;) {
        void *tag;
        bool ok = false;
        // AUTIL_LOG(INFO, "worker cq: %p", &cq);
        if (!cq.Next(&tag, &ok)) {
            break;
        }
        // AUTIL_LOG(INFO, "next %s", ok ? "true" : "false" );
        if (tag) {
            auto closure = static_cast<GigStreamClosureBase *>(tag);
            closure->run(ok);
        } else {
            // shutdown alarm
            autil::ScopedReadWriteLock lock(cqsPtr->enqueueLock, 'w');
            cqsPtr->stopped = true;
            cqsPtr->cq->Shutdown();
            AUTIL_LOG(INFO, "cq: %p shutdown", cqsPtr->cq.get());
        }
    }
    AUTIL_LOG(INFO, "client work loop stopped, worker [%p] idx [%lu] cq [%p]", this, idx, &cq);
}

} // namespace multi_call
