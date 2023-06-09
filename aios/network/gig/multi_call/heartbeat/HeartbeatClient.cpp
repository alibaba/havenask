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
#include "aios/network/gig/multi_call/heartbeat/HeartbeatClient.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, HeartbeatClient);

HeartbeatClient::HeartbeatClient(const ConnectionManagerPtr &cm,
                                 const MiscConfigPtr &cfg)
    : _miscConfig(cfg) {
    _metaManagerClient.reset(new MetaManagerClient(cm));
    _hbThreadPool.reset(new autil::ThreadPool(_miscConfig->heartbeatThreadCount,
                                              DEFAULT_QUEUE_SIZE));
    _stubPool.reset(new HeartbeatStubPool(cm));
}

HeartbeatClient::~HeartbeatClient() {
    if (_hbThreadPool) {
        _hbThreadPool->stop();
    }
}

bool HeartbeatClient::init() {
    if (!_hbThreadPool || !_hbThreadPool->start()) {
        AUTIL_LOG(ERROR, "start hb thread pool failed");
        return false;
    }
    if (!_stubPool->init()) {
        AUTIL_LOG(ERROR, "init stub pool failed");
        return false;
    }
    return true;
}

void HeartbeatClient::fillHbRequest(const std::string &hbSpec,
                                    HeartbeatRequest &hbRequest) {
    _metaManagerClient->fillHbRequest(hbSpec, hbRequest);
}

void HeartbeatClient::processHbResponse(
    const std::string &hbSpec,
    std::vector<SearchServiceProviderPtr> &providerVec,
    HeartbeatResponse &hbResponse) {
    if (hbResponse.has_meta()) {
        _metaManagerClient->updateMetaInfo(hbSpec, providerVec,
                                           hbResponse.meta());
    }
}

void HeartbeatClient::heartbeat(
    const std::string &hbSpec,
    std::vector<SearchServiceProviderPtr> &providerVec) {

    if (hbSpec.empty() || providerVec.empty()) {
        return;
    }
    SearchServiceProviderPtr &firstProvider = providerVec.front();
    if (!firstProvider) {
        return;
    }

    ProtocolType protocolType = firstProvider->getHeartbeatProtocol();

    if (MC_PROTOCOL_ARPC == protocolType) {
        HeartbeatServiceStubPtr stubPtr = _stubPool->getServiceStub(hbSpec);
        if (!stubPtr) {
            AUTIL_LOG(WARN, "get null heartbeat service stub");
            return;
        }
        HeartbeatClientPtr hbClient = this->shared_from_this();
        HeartbeatClosure *heartbeatClosure =
            new HeartbeatClosure(hbSpec, hbClient, providerVec);
        heartbeatClosure->getController()->SetExpireTime(
            _miscConfig->heartbeatTimeout);

        HeartbeatRequest *hbRequest = heartbeatClosure->getRequest();
        fillHbRequest(hbSpec, *hbRequest);

        HeartbeatResponse *hbResponse = heartbeatClosure->getResponse();
        stubPtr->gig_heartbeat(heartbeatClosure->getController(), hbRequest,
                               hbResponse, heartbeatClosure);

    } else if (MC_PROTOCOL_GRPC == protocolType) {
        HeartbeatGrpcStubPtr grpcStubPtr = _stubPool->getGrpcStub(hbSpec);
        if (!grpcStubPtr) {
            AUTIL_LOG(WARN, "get null heartbeat service grpc stub");
            return;
        }
        HeartbeatClientPtr hbClient = this->shared_from_this();
        HeartbeatGrpcClosure *grpcClosure =
            new HeartbeatGrpcClosure(hbSpec, hbClient, providerVec);

        HeartbeatRequest *hbRequest = grpcClosure->getRequest();
        fillHbRequest(hbSpec, *hbRequest);
        grpcClosure->unparseRequestProto();

        grpc::ClientContext *clientContext = grpcClosure->getClientContext();
        auto deadline =
            std::chrono::system_clock::now() +
            std::chrono::milliseconds(_miscConfig->heartbeatTimeout);
        clientContext->set_deadline(deadline);

        auto cq = _stubPool->getGrpcClientWorker()->getCompletionQueue(
            (size_t)this >> 3);
        std::unique_ptr<grpc::GenericClientAsyncResponseReader> rpc(
            grpcStubPtr->PrepareUnaryCall(clientContext, "gig_heartbeat_grpc",
                                          grpcClosure->getRequestBuf(),
                                          cq->cq.get()));
        rpc->StartCall();
        rpc->Finish(grpcClosure->getResponseBuf(), &grpcClosure->getStatus(),
                    grpcClosure);
    } else {
        AUTIL_LOG(WARN, "unsupported hb protocol for spec:[%s], protocol:[%u]",
                  hbSpec.c_str(), protocolType);
        ;
    }
}

bool HeartbeatClient::clearObsoleteSpec(
    const std::unordered_set<std::string> &allSpecSet) {
    return _metaManagerClient->clearObsoleteSpec(allSpecSet) ||
           _stubPool->cleanObsoleteStub(allSpecSet);
}

void HeartbeatClient::fillHbMetaInTopoNode(TopoNodeVec &topoNodeVec) {
    _metaManagerClient->fillMetaInTopoNode(topoNodeVec);
}

} // namespace multi_call
