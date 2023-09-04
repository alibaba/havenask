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
#include "navi/engine/NaviStreamSession.h"
#include "navi/engine/Graph.h"
#include "navi/engine/GraphDomainServer.h"
#include "navi/util/CommonUtil.h"
#include "aios/network/gig/multi_call/interface/QuerySession.h"
#include "autil/CompressionUtil.h"

namespace navi {

NaviStreamSession::NaviStreamSession(const NaviLoggerPtr &loggerPtr,
                                     TaskQueue *taskQueue,
                                     BizManager *bizManager,
                                     NaviMessage *request,
                                     const ArenaPtr &arena,
                                     const multi_call::GigStreamBasePtr &stream)
    : NaviWorkerBase(loggerPtr, taskQueue, bizManager)
    , _arena(arena)
    , _request(request)
    , _stream(stream)
    , _graphDef(nullptr)
    , _graph(nullptr)
{
}

NaviStreamSession::~NaviStreamSession() {
    NAVI_LOG(SCHEDULE1, "destructed");
    clear();
    {
        NaviLoggerScope scope(_logger);
        delete _graph;
    }
}

bool NaviStreamSession::decompressGraph() {
    const auto &domainGraphDef = _request->domain_graph();
    const auto &graphStr = domainGraphDef.sub_graph();
    auto thisPartId = domainGraphDef.this_part_id();
    auto compressType = domainGraphDef.compress_type();
    _graphDef = google::protobuf::Arena::CreateMessage<GraphDef>(_arena.get());
    _graphDef->set_version(_request->version());
    auto subGraphDef = _graphDef->add_sub_graphs();
    if (CT_NONE == compressType) {
        if (!subGraphDef->ParseFromString(graphStr)) {
            NAVI_LOG(ERROR, "parse sub graph def failed");
            return false;
        }
    } else if (CT_LZ4 == compressType) {
        std::string decompressedResult;
        if (!autil::CompressionUtil::decompress(graphStr, autil::CompressType::LZ4, decompressedResult, nullptr)) {
            NAVI_LOG(ERROR, "decompresss sub graph def failed");
            return false;
        }
        if (!subGraphDef->ParseFromString(decompressedResult)) {
            NAVI_LOG(ERROR, "parse sub graph def failed");
            return false;
        }
    }
    subGraphDef->mutable_location()->set_this_part_id(thisPartId);
    auto counterInfo = _graphDef->mutable_counter_info();
    counterInfo->CopyFrom(domainGraphDef.counter_info());
    return true;
}

bool NaviStreamSession::doInit(GraphParam *param) {
    _logger.addPrefix("stream");
    startTrace(param->querySession);
    if (!decompressGraph()) {
        _stream.reset();
        delete param;
        NAVI_LOG(ERROR, "parse sub graph def failed");
        return false;
    }
    NAVI_LOG(SCHEDULE1, "graph before mirror: %s",
             _graphDef->DebugString().c_str());
    MirrorGraphInfoMap mirrorInfoMap;
    addMirrorGraph(mirrorInfoMap);
    _graph = new Graph(param, nullptr);
    initDomainMap(mirrorInfoMap, _stream, _graph);
    _stream.reset();
    NAVI_LOG(SCHEDULE2, "graph after mirror: %s",
             _graphDef->DebugString().c_str());
    return true;
}

void NaviStreamSession::startTrace(const std::shared_ptr<multi_call::QuerySession> &querySession) const {
    if (!querySession) {
        return;
    }
    auto span = querySession->getTraceServerSpan();
    if (!span) {
        return;
    }
    span->setAttribute(opentelemetry::kSpanAttrNetPeerIp, _stream->getPeer());
}

void NaviStreamSession::addMirrorGraph(MirrorGraphInfoMap &mirrorInfoMap) {
    auto subCount = _graphDef->sub_graphs_size();
    std::unordered_set<GraphId> originGraphSet;
    std::unordered_map<GraphId, SubGraphDef *> originGraphMap;
    for (int32_t i = 0; i < subCount; i++) {
        auto subGraphDef = _graphDef->mutable_sub_graphs(i);
        auto graphId = subGraphDef->graph_id();
        originGraphMap.emplace(graphId, subGraphDef);
        originGraphSet.insert(graphId);
    }
    std::unordered_map<GraphId, SubGraphDef *> mirrorGraphMap;
    for (const auto &pair : originGraphMap) {
        auto graphId = pair.first;
        auto subGraphDef = pair.second;
        auto borderCount = subGraphDef->borders_size();
        for (int32_t borderIndex = 0; borderIndex < borderCount; borderIndex++) {
            const auto &borderDef = subGraphDef->borders(borderIndex);
            const auto &peerInfo = borderDef.peer();
            auto peer = peerInfo.graph_id();
            if (originGraphSet.end() != originGraphSet.find(peer)) {
                continue;
            }
            auto newSubGraphDef = getNewSubGraphDef(peer, mirrorGraphMap);
            const auto &bizName = CommonUtil::getMirrorBizName(peer);
            newSubGraphDef->mutable_location()->set_biz_name(bizName);
            *(newSubGraphDef->mutable_location()->mutable_part_info()) = peerInfo.part_info();
            mirrorBorder(graphId, borderDef, newSubGraphDef);
            mirrorInfoMap.emplace(peer, bizName);
        }
    }
}

SubGraphDef *NaviStreamSession::getNewSubGraphDef(
        GraphId graphId, std::unordered_map<GraphId, SubGraphDef *> &graphMap)
{
    auto it = graphMap.find(graphId);
    if (graphMap.end() != it) {
        return it->second;
    }
    auto newSubGraphDef = _graphDef->add_sub_graphs();
    newSubGraphDef->set_graph_id(graphId);
    graphMap.emplace(graphId, newSubGraphDef);
    return newSubGraphDef;
}

void NaviStreamSession::mirrorBorder(GraphId graphId,
                                     const BorderDef &borderDef,
                                     SubGraphDef *subGraphDef)
{
    auto newBorder = subGraphDef->add_borders();
    newBorder->set_io_type(
        CommonUtil::getReverseIoType((IoType)borderDef.io_type()));
    auto peerInfo = newBorder->mutable_peer();
    peerInfo->set_graph_id(graphId);
    newBorder->mutable_edges()->CopyFrom(borderDef.edges());
}

void NaviStreamSession::initDomainMap(
        const MirrorGraphInfoMap &mirrorInfoMap,
        const multi_call::GigStreamBasePtr &stream,
        Graph *graph)
{
    auto domainServer = new GraphDomainServer(graph);
    domainServer->setStream(stream);
    GraphDomainPtr domain(domainServer);
    for (const auto &pair : mirrorInfoMap) {
        const auto &graphId = pair.first;
        graph->setGraphDomain(graphId, domain);
    }
}

void NaviStreamSession::run() {
    NAVI_LOG(SCHEDULE1, "start");
    auto ec = _graph->init(_graphDef);
    if (EC_NONE != ec) {
        _graph->notifyFinish(ec);
        return;
    }
    ec = _graph->run();
    if (EC_NONE != ec) {
        _graph->notifyFinish(ec);
        return;
    }
}

void NaviStreamSession::drop() {
    NAVI_LOG(SCHEDULE1, "dropped");
    auto ec = EC_QUEUE_FULL;
    if (_graph->isTimeout()) {
        ec = EC_TIMEOUT;
    }
    _graph->notifyFinish(ec);
}

bool NaviStreamSession::isTimeout() const {
    auto timeoutChecker = _graph->getTimeoutChecker();
    if (timeoutChecker->timeout()) {
        auto curTime = autil::TimeUtility::currentTime();
        auto beginTime = timeoutChecker->beginTime();
        NAVI_LOG(ERROR,
                 "stream session timeout, expect time [%ld ms] use time [%ld ms], start "
                 "[%ld], end[%ld]",
                 timeoutChecker->timeoutMs(),
                 (curTime - beginTime) / FACTOR_MS_TO_US,
                 beginTime / FACTOR_MS_TO_US,
                 curTime / FACTOR_MS_TO_US);
        NAVI_LOG(SCHEDULE1, "timeout bt: %s",
                 _logger.logger->firstErrorBackTrace().c_str());
        return true;
    }
    return false;
}

}
