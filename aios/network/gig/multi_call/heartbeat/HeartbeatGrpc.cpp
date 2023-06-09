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
#include "aios/network/gig/multi_call/heartbeat/HeartbeatGrpc.h"
#include "aios/network/gig/multi_call/heartbeat/HeartbeatClient.h"
#include "aios/network/gig/multi_call/util/ProtobufByteBufferUtil.h"
#include "autil/ThreadPool.h"
#include <string>

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, HeartbeatGrpcClosure);

HeartbeatGrpcClosure::HeartbeatGrpcClosure(
    const std::string &hbSpec, std::shared_ptr<HeartbeatClient> &client,
    std::vector<SearchServiceProviderPtr> &providerVec)
    : GrpcClientClosure(NULL, CallBackPtr()) // just use empty params to skip
      ,
      _args(new HeartbeatWorkerArgs(hbSpec, client, providerVec)) {}

HeartbeatGrpcClosure::~HeartbeatGrpcClosure() {}

void HeartbeatGrpcClosure::run(bool ok) {
    auto item = new HeartbeatWorkItem(_args);
    grpc::Status &status = getStatus();
    item->setStatusCode(std::string("GRPC_") +
                        std::to_string(status.error_code()));
    if (!ok || !status.ok()) {
        AUTIL_LOG(ERROR, "hb grpc failed, code:%d, msg:%s, peer:%s",
                  status.error_code(), status.error_message().c_str(),
                  getClientContext()->peer().c_str())
        item->setFailed(true);
    }
    grpc::ByteBuffer *responseBuf = getResponseBuf();
    if (!ProtobufByteBufferUtil::deserializeFromBuffer(*responseBuf,
                                                       _args->hbResponse)) {
        AUTIL_LOG(ERROR, "hb grpc parse response failed");
        item->destroy();
        return;
    }
    auto threadPool = _args->hbClient->getHbThreadPool();
    auto errorCode = threadPool->pushWorkItem(item, false);
    if (autil::ThreadPool::ERROR_NONE != errorCode) {
        AUTIL_LOG(ERROR,
                  "push HeartbeatWorkItem to thread pool failed. "
                  "ErrorCode=%d ThreadPoolSize=%ld",
                  errorCode, threadPool->getQueueSize());
        item->drop();
    }
    delete this;
}

} // namespace multi_call
