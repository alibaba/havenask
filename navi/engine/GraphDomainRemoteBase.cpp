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
#include "navi/engine/GraphDomainRemoteBase.h"
#include "navi/distribute/NaviStreamBase.h"
#include "navi/engine/Graph.h"
#include "navi/resource/MetaInfoResourceBase.h"

namespace navi {

GraphDomainRemoteBase::GraphDomainRemoteBase(Graph *graph, GraphDomainType type)
    : GraphDomain(graph, type)
{
}

GraphDomainRemoteBase::~GraphDomainRemoteBase() {
}

bool GraphDomainRemoteBase::preInit() {
    if (EC_NONE != doPreInit()) {
        return false;
    }
    if (!_streamState.init(_pool.get())) {
        NAVI_LOG(ERROR, "init domain part state failed");
        return false;
    }
    if (!bindStream()) {
        NAVI_LOG(ERROR, "bind domain stream failed");
        return false;
    }
    return true;
}

ErrorCode GraphDomainRemoteBase::doPreInit() {
    return EC_NONE;
}

NaviStreamBase *GraphDomainRemoteBase::getStreamBase() {
    auto stream = getStream();
    return dynamic_cast<NaviStreamBase *>(stream.get());
}

bool GraphDomainRemoteBase::bindStream() {
    auto naviStream = getStreamBase();
    NAVI_LOG(SCHEDULE1, "bind stream [%p]", naviStream);
    return naviStream->setDomain(shared_from_this());
}

void GraphDomainRemoteBase::unBindStream() {
    auto naviStream = getStreamBase();
    NAVI_LOG(SCHEDULE1, "unbind stream [%p]", naviStream);
    if (naviStream) {
        naviStream->finish();
    }
}

void GraphDomainRemoteBase::notifyIdle(NaviPartId partId) {
}

void GraphDomainRemoteBase::setStream(
    const multi_call::GigStreamBasePtr &stream)
{
    NAVI_LOG(SCHEDULE1, "set stream [%p]", stream.get());
    autil::ScopedWriteLock lock(_streamLock);
    if (!stream) {
        NAVI_LOG(DEBUG, "stream reset, last stream [%p], use_count: %lu",
                 _stream.get(), _stream.use_count());
    }
    _stream = stream;
}

multi_call::GigStreamBasePtr GraphDomainRemoteBase::getStream() const {
    autil::ScopedReadLock lock(_streamLock);
    return _stream;
}

void GraphDomainRemoteBase::notifySend(NaviPartId partId) {
    if (!acquire(GDHR_NOTIFY_SEND)) {
        return;
    }
    send(partId);
    auto worker = release(GDHR_NOTIFY_SEND);
    if (worker) {
        worker->decItemCount();
    }
}

void GraphDomainRemoteBase::send(NaviPartId partId) {
    NAVI_LOG(SCHEDULE2, "send to partId [%d]", partId);
    auto gigPartId = getStreamPartId(partId);
    bool hasError = false;
    while (true) {
        if (_streamState.noMessage(gigPartId)) {
            break;
        }
        SendScope scope(_streamState, gigPartId);
        if (!scope.success()) {
            break;
        }
        if (_streamState.noMessage(gigPartId)) {
            continue;
        }
        NaviMessage naviMessage;
        if (!collectData(partId, naviMessage)) {
            NAVI_LOG(ERROR, "collect data failed");
            hasError = true;
            break;
        }
        if (!_streamState.cancelled(gigPartId)) {
            if (!doSend(gigPartId, naviMessage) && !_streamState.cancelled(gigPartId)) {
                NAVI_LOG(WARN, "do send failed or partState cancelled, partId [%d]", gigPartId);
                if (onSendFailure(gigPartId)) {
                    hasError = true;
                    break;
                }
            }
        }
    }
    if (hasError) {
        notifyFinish(EC_STREAM_SEND, true);
    }
}

bool GraphDomainRemoteBase::collectData(NaviPartId partId,
                                        NaviMessage &naviMessage)
{
    for (const auto &pair : _remoteInputBorderMap) {
        const auto &border = pair.second;
        NaviBorderData borderData;
        if (!border->collectBorderMessage(partId, borderData)) {
            return false;
        }
        if (0 == borderData.port_datas_size()) {
            continue;
        }
        naviMessage.add_border_datas()->Swap(&borderData);
    }
    return true;
}

bool GraphDomainRemoteBase::receiveCancel(
        const multi_call::GigStreamMessage &message,
        multi_call::MultiCallErrorCode ec)
{
    NAVI_LOG(
        DEBUG, "receive cancel, handler: [%p], ec [%s]", (void *)message.handlerId, multi_call::translateErrorCode(ec));
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

    logCancel(message, ec);
    beforePartReceiveFinished(partId);
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
    receiveCallBack(partId);
    return true;
}

bool GraphDomainRemoteBase::receive(
    const multi_call::GigStreamMessage &message)
{
    if (message.eof) {
        _streamState.setReceiveEof(message.partId);
        beforePartReceiveFinished(message.partId);
    }
    if (!doReceive(message)) {
        NAVI_LOG(ERROR, "receive msg failed");
        notifyFinish(EC_STREAM_RECEIVE, true);
        return false;
    }
    receiveCallBack(message.partId);
    return true;
}

bool GraphDomainRemoteBase::doReceive(
    const multi_call::GigStreamMessage &message)
{

    auto naviMessage = dynamic_cast<NaviMessage *>(message.message);
    if (!naviMessage) {
        NAVI_LOG(ERROR, "receive error, null msg, partId[%d], this type: %s",
                 message.partId, typeid(*this).name());
        return false;
    }
    NAVI_LOG(SCHEDULE2, "message data receive [%08x] partId: %d, msg len: %d, eof: %d",
             naviMessage->msg_id(), message.partId,
             message.message->ByteSize(), message.eof);
    NAVI_LOG(SCHEDULE3,
             "message data receive [%08x] partId: %d, handler: [%p], msg content [%s]",
             naviMessage->msg_id(),
             message.partId,
             (void *)message.handlerId,
             message.message->DebugString().c_str());
    receivePartResult(naviMessage);
    receivePartTrace(naviMessage);
    if (!receivePartMetaInfo(naviMessage)) {
        NAVI_LOG(ERROR, "receive meta info failed");
        return false;
    }

    auto borderCount = naviMessage->border_datas_size();
    for (int32_t index = 0; index < borderCount; index++) {
        auto &borderData = *naviMessage->mutable_border_datas(index);
        if (!borderReceive(borderData)) {
            return false;
        }
    }
    return true;
}

bool GraphDomainRemoteBase::borderReceive(NaviBorderData &borderData) {
    BorderId borderId(borderData.border_id());
    auto border = getOutputBorder(borderId);
    if (!border) {
        NAVI_LOG(ERROR, "border id not found: %s",
                 autil::legacy::ToJsonString(BorderIdJsonize(borderId)).c_str());
        return false;
    }
    return border->receive(borderData);
}

void GraphDomainRemoteBase::receivePartTrace(NaviMessage *naviMessage) {
    const auto &traceStr = naviMessage->navi_trace();
    if (traceStr.empty()) {
        return;
    }
    try {
        autil::DataBuffer dataBuffer((void *)traceStr.data(),
                                     traceStr.size());
        auto worker = _param->worker;
        TraceCollector collector;
        dataBuffer.read(collector);
        NAVI_LOG(SCHEDULE2, "receive part trace [%lu]", collector.eventSize());
        worker->appendTrace(collector);
    } catch (const autil::legacy::ExceptionBase &e) {
    }
}

bool GraphDomainRemoteBase::receivePartMetaInfo(NaviMessage *naviMessage) {
    const auto &metaInfoStr = naviMessage->navi_meta_info();
    if (metaInfoStr.empty()) {
        return true;
    }
    NAVI_LOG(SCHEDULE1, "meta info string received, length [%lu]",
             metaInfoStr.size());
    auto metaInfoResource = _param->worker->getMetaInfoResource();
    if (!metaInfoResource) {
        NAVI_LOG(ERROR, "meta info resource not exist, ignore, length [%lu]",
                 metaInfoStr.size());
        return false;
    }
    try {
        autil::DataBuffer dataBuffer((void *)metaInfoStr.data(),
                                     metaInfoStr.size());
        if (!metaInfoResource->merge(dataBuffer)) {
            NAVI_LOG(ERROR, "meta info resource merge failed, length [%lu]",
                     metaInfoStr.size());
            return false;
        }
    } catch (const autil::legacy::ExceptionBase &e) {
        NAVI_LOG(ERROR, "meta info resource catch exception [%s]",
                 e.ToString().c_str());
        return false;
    }

    return true;
}

void GraphDomainRemoteBase::receivePartResult(NaviMessage *naviMessage) {
    const auto &resultStr = naviMessage->navi_result();
    if (resultStr.empty()) {
        return;
    }
    try {
        autil::DataBuffer dataBuffer((void *)resultStr.data(),
                                     resultStr.size());
        NaviResult result;
        dataBuffer.read(result);
        NAVI_LOG(SCHEDULE2, "receive part trace from navi result [%lu]", result.traceCollector.eventSize());
        auto ec = result.ec;
        if (ec == EC_TIMEOUT || ec == EC_PART_CANCEL) {
            NAVI_LOG(DEBUG, "ignore ec [%s] as part lack", CommonUtil::getErrorString(ec));
            result.ec = EC_NONE;
        }
        _param->worker->appendResult(result);
    } catch (const autil::legacy::ExceptionBase &e) {
    }
}

void GraphDomainRemoteBase::doFinish(ErrorCode ec) {
    NAVI_LOG(SCHEDULE1, "do finish, ec [%s]", CommonUtil::getErrorString(ec));
    unBindStream();
    auto stream = getStream();
    if (stream) {
        tryCancel(stream);
    }
    setStream(multi_call::GigStreamBasePtr());
}

SubGraphBorder *GraphDomainRemoteBase::getOutputBorder(
        const BorderId &borderId) const
{
    auto it = _outputBorderMap.find(borderId);
    if (_outputBorderMap.end() == it) {
        return nullptr;
    } else {
        return it->second.get();
    }
}

void GraphDomainRemoteBase::incMessageCount(NaviPartId partId) {
    _streamState.incMessageCount(getStreamPartId(partId));
}

void GraphDomainRemoteBase::decMessageCount(NaviPartId partId) {
    _streamState.decMessageCount(getStreamPartId(partId));
}

}
