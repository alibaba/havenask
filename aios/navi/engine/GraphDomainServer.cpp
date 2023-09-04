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
#include "navi/engine/GraphDomainServer.h"
#include "navi/engine/RemoteInputPort.h"
#include "navi/engine/RemoteOutputPort.h"
#include "navi/engine/Graph.h"
#include "aios/network/gig/multi_call/util/FileRecorder.h"
#include "navi/proto/GraphVis.pb.h"

namespace navi {

GraphDomainServer::GraphDomainServer(Graph *graph)
    : GraphDomainRemoteBase(graph, GDT_SERVER)
{
}

GraphDomainServer::~GraphDomainServer() {
}

ErrorCode GraphDomainServer::doPreInit() {
    _logger.addPrefix("onBiz: %s part: %d",
                      _graph->getLocalBizName().c_str(),
                      _graph->getLocalPartId());
    return EC_NONE;
}

bool GraphDomainServer::run() {
    _streamState.setInited(0);
    return true;
}

bool GraphDomainServer::doSend(NaviPartId gigPartId, NaviMessage &naviMessage) {
    if (0 == naviMessage.border_datas_size()) {
        return true;
    }
    naviMessage.set_msg_id(CommonUtil::random64());
    bool eof = sendEof();
    NAVI_LOG(SCHEDULE1, "start do send, eof [%d]", eof);
    auto localPartId = _graph->getLocalPartId();
    if (eof) {
        NAVI_LOG(SCHEDULE1, "send eof from localPartId [%d]", localPartId);
        _streamState.setSendEof(gigPartId);
        fillFiniMessage(naviMessage);
    } else if (_streamState.sendEof(gigPartId)) {
        NAVI_LOG(SCHEDULE1, "part [%d] already finished, msg [%s]", gigPartId,
                 naviMessage.DebugString().c_str());
        return false;
    }
    fillTrace(naviMessage);
    NAVI_LOG(SCHEDULE2, "message data send [%08x] partId: %d, msg len: %d, eof: %d",
             naviMessage.msg_id(), localPartId,
             naviMessage.ByteSize(), eof);
    NAVI_LOG(SCHEDULE3, "message data send [%08x] partId: %d, eof [%d] msg content [%s]",
             naviMessage.msg_id(), localPartId,
             eof, naviMessage.DebugString().c_str());
    {
        auto stream = getStream();
        if (!stream) {
            NAVI_LOG(SCHEDULE1, "part [%d] null stream, msg [%s]", gigPartId,
                     naviMessage.DebugString().c_str());
            return false;
        }
        if (!stream->send(gigPartId, eof, &naviMessage)) {
            NAVI_LOG(SCHEDULE1, "part [%d] gig stream send data failed, msg [%s]",
                     gigPartId, naviMessage.DebugString().c_str());
            return false;
        }
    }
    if (eof) {
        notifyFinish(EC_NONE, true);
    }
    return true;
}

bool GraphDomainServer::receiveCancel(const multi_call::GigStreamMessage &message, multi_call::MultiCallErrorCode ec) {
    NAVI_LOG(DEBUG,
             "server receive cancel, handler: [%p], ec [%s], partId: %d",
             (void *)message.handlerId,
             multi_call::translateErrorCode(ec),
             message.partId);
    auto partId = message.partId;
    bool cancelled;
    {
        StreamStatePartScopedLock lock(_streamState, partId);
        cancelled = _streamState.cancelled(partId);
        _streamState.receiveCancel(partId);
    }
    if (cancelled) {
        NAVI_LOG(SCHEDULE2, "ignore cancel after stream state cancelled, partId [%d]", partId);
        return true;
    }
    if (message.message) {
        NAVI_LOG(SCHEDULE2, "cancel msg len [%d]",
                 message.message->ByteSize());
        NAVI_LOG(SCHEDULE3, "cancel msg content [%s]",
                 message.message->DebugString().c_str());
        if (!doReceive(message)) {
            notifyFinish(EC_STREAM_RPC, true);
            return false;
        }
    }
    notifyFinish(EC_ABORT, true);
    return true;
}

bool GraphDomainServer::receive(const multi_call::GigStreamMessage &message) {
    if (message.eof) {
        _streamState.setReceiveEof(message.partId);
    }
    if (!doReceive(message)) {
        NAVI_LOG(ERROR, "receive msg failed");
        notifyFinish(EC_STREAM_RECEIVE, true);
        return false;
    }
    if (_streamState.cancelled(message.partId)) {
        notifyFinish(EC_ABORT, true);
    }
    return true;
}

void GraphDomainServer::fillTrace(NaviMessage &naviMessage) {
    TraceCollector collector;
    _logger.logger->collectTrace(collector);
    if (!collector.empty()) {
        NAVI_LOG(SCHEDULE2, "fill part trace [%lu]", collector.eventSize());
        autil::DataBuffer dataBuffer(4 * 1024);
        collector.serialize(dataBuffer);
        assert(naviMessage.navi_trace().empty() && "trace field not empty");
        naviMessage.set_navi_trace(dataBuffer.getData(), dataBuffer.getDataLen());
    }
}

void GraphDomainServer::fillFiniMessage(NaviMessage &naviMessage) {
    NAVI_LOG(SCHEDULE1, "start fill finish message [%08x]", naviMessage.msg_id());
    auto worker = _param->worker;
    worker->fillResult();
    auto userResult = worker->getUserResult();
    assert(userResult);
    auto naviResult = userResult->getNaviResult();
    assert(naviResult);
    NAVI_LOG(SCHEDULE2, "fill part trace from navi result [%lu]", naviResult->traceCollector.eventSize());
    autil::DataBuffer dataBuffer(autil::DataBuffer::DEFAUTL_DATA_BUFFER_SIZE);
    if (_param->runParams.collectMetric() || _param->runParams.collectPerf()) {
        std::shared_ptr<navi::GraphVisDef> visProto = naviResult->getGraphVisData();
        const std::string &visStr = visProto->SerializeAsString();
        multi_call::FileRecorder::newRecord(
                visStr, 5,
                "timeline",
                "searcher_graph_vis_" + autil::StringUtil::toString(getpid()));
    }
    try {
        naviResult->serialize(dataBuffer);
        std::string data(dataBuffer.getData(), dataBuffer.getDataLen());
        naviMessage.set_navi_result(data);
    } catch (const autil::legacy::ExceptionBase &e) {
    }
    NAVI_LOG(SCHEDULE1, "end fill finish message [%08x]", naviMessage.msg_id());
}

NaviPartId GraphDomainServer::getStreamPartId(NaviPartId partId) {
    return 0;
}

void GraphDomainServer::tryCancel(const multi_call::GigStreamBasePtr &stream) {
    NaviPartId partId = 0;
    auto inited = _streamState.streamInited();
    if (inited) {
        if (_streamState.cancelled(partId)) {
            return;
        }
        if (_streamState.sendEof(partId)) {
            return;
        }
        _streamState.lock(partId);
    }
    NaviMessage naviMessage;
    naviMessage.set_msg_id(CommonUtil::random64());
    fillFiniMessage(naviMessage);
    NAVI_LOG(SCHEDULE3, "send cancel data [%s]", naviMessage.DebugString().c_str());
    stream->sendCancel(partId, &naviMessage);
    if (inited) {
        _streamState.unlock(partId);
    }
}

bool GraphDomainServer::onSendFailure(NaviPartId gigPartId) {
    // trigger run graph error
    return true;
}

bool GraphDomainServer::sendEof() const {
    bool eof = true;
    for (const auto &pair : _remoteInputBorderMap) {
        eof &= pair.second->eof();
    }
    return eof;
}

}
