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
#include "navi/engine/GraphDomainClient.h"

#include "aios/network/opentelemetry/core/eagleeye/EagleeyeUtil.h"
#include "multi_call/interface/QuerySession.h"
#include "navi/distribute/NaviClientStream.h"
#include "navi/engine/Graph.h"

using namespace autil;

namespace navi {

class NaviClientStreamDestructItem : public NaviThreadPoolItemBase
{
public:
    NaviClientStreamDestructItem(NaviClientStream *stream)
        : _stream(stream)
    {
    }
public:
    void process() override {
        NaviClientStream *stream = nullptr;
        std::swap(stream, _stream);
        if (stream) {
            delete stream;
        }
    }
    void destroy() override {
        process();
        delete this;
    }
private:
    NaviClientStream *_stream = nullptr;
};

class NotifyFinishItem : public NaviThreadPoolItemBase
{
public:
    NotifyFinishItem(GraphDomainClient *domain, ErrorCode ec)
        : _domain(domain)
        , _ec(ec)
    {
    }
public:
    void process() override {
        GraphDomainClient *domain = nullptr;
        std::swap(domain, _domain);
        if (domain) {
            domain->notifyFinish(_ec, false);
            auto *worker = domain->release(GDHR_NOTIFY_FINISH_ASYNC);
            if (worker) {
                worker->decItemCount();
            }
        }
    }
    void destroy() override {
        process();
        delete this;
    }
private:
    GraphDomainClient *_domain = nullptr;
    ErrorCode _ec;
};

GraphDomainClient::GraphDomainClient(Graph *graph,
                                     const std::string &bizName)
    : GraphDomainRemoteBase(graph, GDT_CLIENT)
    , _bizName(bizName)
{
    _logger.addPrefix("biz: %s", bizName.c_str());
}

GraphDomainClient::~GraphDomainClient() {
}

ErrorCode GraphDomainClient::doPreInit() {
    assert(!_subGraphs.empty());

    if (!checkSubGraphLocation()) {
        NAVI_LOG(ERROR, "check subgraph locations failed");
        return EC_UNKNOWN;
    }

    auto *firstSubGraphDef = _subGraphs[0];
    if (!initGigStream(firstSubGraphDef->location())) {
        NAVI_LOG(ERROR, "init gig stream failed, biz: %s", _bizName.c_str());
        return EC_UNKNOWN;
    }
    return EC_NONE;
}

bool GraphDomainClient::checkSubGraphLocation() const {
    auto *firstSubGraphDef = _subGraphs[0];
    for (auto *subGraphDef : _subGraphs) {
        const auto &firstPartIds = firstSubGraphDef->location().part_info().part_ids();
        const auto &subPartIds = subGraphDef->location().part_info().part_ids();
        if (firstPartIds.size() != subPartIds.size()
            || !std::equal(firstPartIds.begin(), firstPartIds.end(), subPartIds.begin())) {
            NAVI_LOG(ERROR, "subGraph location mismatch. [%s] vs [%s]",
                     firstSubGraphDef->location().ShortDebugString().c_str(),
                     subGraphDef->location().ShortDebugString().c_str());
            return false;
        }
    }
    return true;
}

bool GraphDomainClient::initGigStream(const LocationDef &location) {
    auto querySession = _param->querySession;
    if (!querySession) {
        NAVI_LOG(ERROR, "get query session failed for client biz [%s]", _bizName.c_str());
        return false;
    }
    auto span = querySession->getTraceServerSpan();
    NAVI_LOG(SCHEDULE1,
             "get query session with eTraceId[%s]",
             span ? opentelemetry::EagleeyeUtil::getETraceId(span).c_str() : "");

    auto stream = createClientStream();
    const auto &partInfoDef = location.part_info();
    for (const auto &partId : partInfoDef.part_ids()) {
        stream->enablePartId(partId);
    }
    stream->setDisableProbe(!location.gig_info().enable_probe());
    addTags(stream, location.gig_info());
    stream->setTimeout(_param->worker->getTimeoutChecker()->remainTime());
    auto gigStream =
        std::dynamic_pointer_cast<multi_call::GigClientStream>(stream);
    if (!querySession->bind(gigStream)) {
        NAVI_LOG(WARN, "bind gig stream failed, maybe biz not exist or full degrade");
        return false;
    }
    auto partCount = std::max(1, stream->getPartCount());
    _partInfo.init(partCount, partInfoDef);
    _streamState.setPartCount(_partInfo.getFullPartCount());

    setStream(gigStream);
    NAVI_LOG(SCHEDULE1, "init gig stream success [%p]", stream.get());
    return true;
}

void GraphDomainClient::addTags(const NaviClientStreamPtr &stream, const GigInfoDef &gigInfo) const {
    const auto &tags = gigInfo.tags();
    for (const auto &tag : tags) {
        stream->addMatchTag(tag.first, (multi_call::TagMatchType)tag.second);
    }
}

bool GraphDomainClient::postInit() {
    auto partCount = _partInfo.getFullPartCount();
    for (auto subGraphDef : _subGraphs) {
        auto location = subGraphDef->mutable_location();
        assert(INVALID_NAVI_PART_COUNT == location->part_info().part_count());
        location->mutable_part_info()->set_part_count(partCount);
        NAVI_LOG(SCHEDULE1, "graph: %d, location: %s", subGraphDef->graph_id(),
                 location->DebugString().c_str());
        if (!initPeerInfo(subGraphDef)) {
            return false;
        }
    }
    return true;
}

bool GraphDomainClient::run() {
    if (finished()) {
        return true;
    }

    for (auto partId : _partInfo) {
        notifySend(partId);
    }
    return true;
}

bool GraphDomainClient::doSend(NaviPartId gigPartId, NaviMessage &naviMessage) {
    if (!_streamState.inited(gigPartId)) {
        NAVI_LOG(SCHEDULE2, "domain client init gigPartId[%d]", gigPartId);
        if (!fillInitMessage(gigPartId, naviMessage)) {
            NAVI_LOG(ERROR, "fill init message failed! gig partId: %d",
                     gigPartId);
            return false;
        }
        _streamState.setInited(gigPartId);
    } else if (0 == naviMessage.border_datas_size()) {
        return true;
    }
    auto stream = getStream();
    if (!stream) {
        NAVI_LOG(SCHEDULE1, "send message failed, null stream! gig partId: %d",
                 gigPartId);
        return true;
    }
    if (_streamState.sendEof(gigPartId)) {
        NAVI_LOG(ERROR, "message after eof, gig partId: %d, message: %s",
                 gigPartId, naviMessage.DebugString().c_str());
        notifyFinish(EC_STREAM_SEND, true);
        return false;
    }
    bool eof = sendEof(gigPartId);
    if (eof) {
        _streamState.setSendEof(gigPartId);
    }
    naviMessage.set_msg_id(CommonUtil::random64());
    NAVI_LOG(SCHEDULE2, "message data send [%08x] toPartId: %d, msg len: %d, eof: %d",
             naviMessage.msg_id(), gigPartId,
             naviMessage.ByteSize(), eof);
    NAVI_LOG(SCHEDULE3, "message data send [%08x] toPartId: %d, eof [%d] msg content [%s]",
             naviMessage.msg_id(), gigPartId,
             eof, naviMessage.DebugString().c_str());
    if (!stream->send(gigPartId, eof, &naviMessage)) {
        auto *clientStream = (NaviClientStream *)stream.get();
        NAVI_LOG(WARN, "stream send message failed, gigPartId [%d], rpc info [%s]",
                 gigPartId,
                 StringUtil::toString(clientStream->getStreamRpcInfo(gigPartId)).c_str());
        return false;
    }
    return true;
}

bool GraphDomainClient::fillInitMessage(NaviPartId partId,
                                        NaviMessage &naviMessage) const
{
    assert(partId < _partInfo.getFullPartCount());
    auto graphDef = naviMessage.mutable_graph();
    std::set<GraphId> graphIdSet;
    for (auto subGraphDef : _subGraphs) {
        auto def = graphDef->add_sub_graphs();
        def->CopyFrom(*subGraphDef);
        def->mutable_location()->set_this_part_id(partId);
        graphIdSet.insert(subGraphDef->graph_id());
    }
    const std::vector<OverrideData> *forkOverrideEdgeDatas = nullptr;
    if (_graph->isForkGraph()) {
        forkOverrideEdgeDatas = &_graph->getOverrideData();
    }
    auto worker = _param->worker;
    return RunGraphParams::toProto(worker->getRunParams(),
                                   forkOverrideEdgeDatas,
                                   graphIdSet,
                                   _param->creatorManager,
                                   partId,
                                   *naviMessage.mutable_params());
}

void GraphDomainClient::tryCancel(const multi_call::GigStreamBasePtr &stream) {
    auto inited = _streamState.streamInited();
    auto partCount = _streamState.getPartCount();
    for (NaviPartId partId = 0; partId < partCount; partId++) {
        StreamStatePartScopedLock lock(_streamState, partId);

        NAVI_LOG(SCHEDULE1,
                 "partId: %d, sendEof: %d, used: %d, cancelled: %d, inited: %d, stream: [%p]",
                 partId, _streamState.sendEof(partId),
                 _partInfo.isUsed(partId), _streamState.cancelled(partId),
                 inited, stream.get());
        if (_partInfo.isUsed(partId) && !_streamState.cancelled(partId)) {
            NAVI_LOG(SCHEDULE1, "try send cancel, partId: %d", partId);
            stream->sendCancel(partId, nullptr);
        }
    }
}

bool GraphDomainClient::onSendFailure(NaviPartId gigPartId) {
    assert(_partInfo.isUsed(gigPartId));
    bool cancelled;
    {
        StreamStatePartScopedLock lock(_streamState, gigPartId);
        cancelled = _streamState.cancelled(gigPartId);
        _streamState.receiveCancel(gigPartId);
    }
    auto stream = getStream();
    NAVI_LOG(SCHEDULE1,
             "partId: %d, sendEof: %d, used: %d, cancelled: %d, inited: %d, stream: [%p]",
             gigPartId, _streamState.sendEof(gigPartId),
             _partInfo.isUsed(gigPartId), cancelled,
             _streamState.streamInited(), stream.get());
    if (cancelled) {
        NAVI_LOG(SCHEDULE2, "ignore send failure after stream state cancelled, partId [%d]",
                 gigPartId);
    } else {
        collectRpcInfo(gigPartId);
        receiveCallBack(gigPartId);

        if (stream) {
            NAVI_LOG(SCHEDULE1, "try send cancel, partId: %d", gigPartId);
            StreamStatePartScopedLock lock(_streamState, gigPartId);
            stream->sendCancel(gigPartId, nullptr);
        }
    }
    // do not trigger run graph error
    return false;
}

const PartInfo &GraphDomainClient::getPartInfo() const {
    return _partInfo;
}

void GraphDomainClient::logCancel(const multi_call::GigStreamMessage &message,
                                  multi_call::MultiCallErrorCode ec)
{
    NAVI_LOG(DEBUG, "client receive cancel, gig ec[%s], partId: %d",
             multi_call::translateErrorCode(ec), message.partId);
}

void GraphDomainClient::beforePartReceiveFinished(NaviPartId partId) {
    collectRpcInfo(partId);
}

void GraphDomainClient::collectRpcInfo(NaviPartId partId) {
    NAVI_LOG(SCHEDULE2, "try collect stream rpc info part [%d]", partId);
    auto stream = getStream();
    if (!stream) {
        NAVI_LOG(SCHEDULE2, "collect rpc info failed, null stream, part [%d]", partId);
        return;
    }

    auto *clientStream = (NaviClientStream *)stream.get();
    auto rpcInfo = clientStream->getStreamRpcInfo(partId);
    NAVI_LOG(SCHEDULE2, "stream rpc info collected part [%d]", partId);

    auto *graphParam = _graph->getGraphParam();
    char buf[256];
    snprintf(buf, 256, "%s(%d)",
             _graph->getLocalBizName().c_str(),
             _graph->getLocalPartId());
    graphParam->worker->getUserResult()->appendRpcInfo(
            std::pair(buf, _bizName), std::move(rpcInfo));
}

void GraphDomainClient::receiveCallBack(NaviPartId gigPartId) {
    assert(_partInfo.isUsed(gigPartId));
    if (!_streamState.receiveEof(gigPartId) && _streamState.cancelled(gigPartId)) {
        NAVI_LOG(DEBUG, "client receive cancel, finish all port, partId [%d]",
                 gigPartId);
        for (const auto &pair : _outputBorderMap) {
            pair.second->setEof(gigPartId);
        }
    }
    if (receiveEof()) {
        NAVI_LOG(SCHEDULE1, "receive all part eof, current partId [%d], partInfo [%s]",
                 gigPartId, _partInfo.ShortDebugString().c_str());
        notifyFinishAsync(EC_NONE);
    }
}

NaviPartId GraphDomainClient::getStreamPartId(NaviPartId partId) {
    return partId;
}

bool GraphDomainClient::sendEof(NaviPartId partId) const {
    bool eof = true;
    for (const auto &pair : _remoteInputBorderMap) {
        eof &= pair.second->eof(partId);
    }
    return eof;
}

bool GraphDomainClient::receiveEof() const {
    bool eof = true;
    for (const auto &pair : _remoteOutputBorderMap) {
        eof &= pair.second->eof();
    }
    return eof;
}

void GraphDomainClient::notifyFinishAsync(ErrorCode ec) {
    if (!acquire(GDHR_NOTIFY_FINISH_ASYNC)) {
        // TODO: assert
        NAVI_LOG(ERROR, "fatal: acquire failed");
        return;
    }
    auto *item = new NotifyFinishItem(this, ec);
    _param->worker->getDestructThreadPool()->push(item);
}

NaviClientStreamPtr GraphDomainClient::createClientStream() {
    const auto &destructThreadPool = _param->worker->getDestructThreadPool();
    NaviClientStreamPtr stream(new NaviClientStream(_bizName, _logger.logger),
                               [destructThreadPool] (NaviClientStream *stream) {
                                   auto *item = new NaviClientStreamDestructItem(stream);
                                   destructThreadPool->push(item);
                               });
    stream->setThreadPool(destructThreadPool);
    stream->setAsyncMode(_param->worker->getRunParams().asyncIo());
    return stream;
}

}
