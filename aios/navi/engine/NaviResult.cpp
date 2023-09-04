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
#include "navi/engine/NaviResult.h"
#include "navi/log/Layout.h"
#include "navi/proto/GraphVis.pb.h"
#include "navi/util/CommonUtil.h"
#include "autil/StringUtil.h"
#include <iostream>

using namespace autil;
using namespace multi_call;

namespace navi {

NaviResult::NaviResult()
    : ec(EC_NONE)
    , collectMetric(false)
{
}

NaviResult::~NaviResult() {
}

void NaviResult::setSessionId(SessionId id_) {
    id = id_;
}

void NaviResult::merge(TraceCollector &collector) {
    autil::ScopedLock lock(_mergeLock);
    traceCollector.merge(collector);
}

void NaviResult::fillTrace(TraceCollector &collector) {
    autil::ScopedLock lock(_mergeLock);
    collector.merge(traceCollector);
}

void NaviResult::merge(NaviResult &other) {
    autil::ScopedLock lock(_mergeLock);
    bool useOther = false;
    if (EC_NONE == ec) {
        if (EC_NONE == other.ec) {
            if (errorEvent.message.empty()) {
                useOther = true;
            }
        } else {
            useOther = true;
        }
    }
    if (useOther) {
        ec = other.ec;
        errorEvent = std::move(other.errorEvent);
        errorBackTrace = std::move(other.errorBackTrace);
    }
    traceCollector.merge(other.traceCollector);
    graphMetric.merge(other.graphMetric);
    rpcInfoMap.merge(other.rpcInfoMap);
}

LoggingEvent NaviResult::getErrorEvent() const {
    autil::ScopedLock lock(_mergeLock);
    return errorEvent;
}

void NaviResult::appendRpcInfo(const multi_call::GigStreamRpcInfoKey &key,
                               multi_call::GigStreamRpcInfo info)
{
    autil::ScopedLock lock(_mergeLock);
    auto it = rpcInfoMap.find(key);
    if (it != rpcInfoMap.end()) {
        it->second.emplace_back(std::move(info));
    } else {
        rpcInfoMap[key] = {std::move(info)};
    }
}

GraphMetric *NaviResult::getGraphMetric() {
    return &graphMetric;
}

void NaviResult::setCollectMetric(bool collect) {
    collectMetric = collect;
}

void NaviResult::setTraceFormatPattern(const std::string &pattern) {
    traceCollector.setFormatPattern(pattern);
}

void NaviResult::end() {
    autil::ScopedLock lock(_mergeLock);
    if (_graphMetricTime.computeUs == 0) {
        _graphMetricTime = graphMetric.end();
    }
}

void NaviResult::serialize(autil::DataBuffer &dataBuffer) const {
    autil::ScopedLock lock(_mergeLock);
    dataBuffer.write(ec);
    dataBuffer.write(errorEvent);
    dataBuffer.write(errorBackTrace);
    dataBuffer.write(traceCollector);
    dataBuffer.write(rpcInfoMap);
    if (collectMetric) {
        dataBuffer.write(true);
        dataBuffer.write(graphMetric);
    } else {
        dataBuffer.write(false);
    }
}

void NaviResult::deserialize(autil::DataBuffer &dataBuffer) {
    autil::ScopedLock lock(_mergeLock);
    dataBuffer.read(ec);
    dataBuffer.read(errorEvent);
    dataBuffer.read(errorBackTrace);
    dataBuffer.read(traceCollector);
    dataBuffer.read(rpcInfoMap);
    dataBuffer.read(collectMetric);
    if (collectMetric) {
        dataBuffer.read(graphMetric);
    }
}

void NaviResult::serializeToString(std::string &data) const {
    DataBuffer dataBuffer(DataBuffer::DEFAUTL_DATA_BUFFER_SIZE);
    serialize(dataBuffer);
    data.clear();
    data.append(dataBuffer.getData(), dataBuffer.getDataLen());
}

void NaviResult::deserializeFromString(const std::string &data) {
    DataBuffer dataBuffer((void*)data.c_str(), data.size());
    deserialize(dataBuffer);
}

GraphMetricTime NaviResult::getGraphMetricTime() const {
    autil::ScopedLock lock(_mergeLock);
    return _graphMetricTime;
}

GigStreamRpcInfoMap NaviResult::stealRpcInfoMap() const {
    autil::ScopedLock lock(_mergeLock);
    return std::move(rpcInfoMap);
}

multi_call::GigStreamRpcInfoMap NaviResult::getRpcInfoMap() const {
    autil::ScopedLock lock(_mergeLock);
    return rpcInfoMap;
}

void NaviResult::setRpcInfoMap(GigStreamRpcInfoMap rpcInfoMap_) {
    autil::ScopedLock lock(_mergeLock);
    rpcInfoMap = std::move(rpcInfoMap_);
}

std::shared_ptr<GraphVisDef> NaviResult::getGraphVisData() const {
    std::shared_ptr<GraphVisDef> vis(new GraphVisDef());
    fillGraphSummary(vis.get());
    traceCollector.fillProto(vis->mutable_trace());
    graphMetric.fillProto(vis->mutable_metric());
    return vis;
}

void NaviResult::fillGraphSummary(GraphVisDef *vis) const {
    auto summary = vis->mutable_summary();
    summary->mutable_session()->set_instance(id.instance);
    summary->mutable_session()->set_query_id(id.queryId);
    summary->set_ec(CommonUtil::getErrorString(ec));
    errorEvent.fillProto(summary->mutable_error_event());
}

void NaviResult::show() const {
    PatternLayout layout;
    layout.setLogPattern(DEFAULT_LOG_PATTERN);
    std::string msg;
    if (!errorEvent.message.empty()) {
        msg = layout.format(errorEvent);
    }

    std::cout << "session: " << CommonUtil::formatSessionId(id, id.instance)
              << ", ec: " << CommonUtil::getErrorString(ec)
              << ", msg: " << msg
              << ", bt: " << errorBackTrace << std::endl;
    std::cout << "rpcInfo: " << std::endl;
    for (const auto &pair : rpcInfoMap) {
        std::cout << "[" << pair.first.first << "->" << pair.first.second << "] " << std::endl
                  << autil::StringUtil::toString(pair.second, "\n") << std::endl;
    }

    std::vector<std::string> traceVec;
    traceCollector.format(traceVec);
    std::cout << "trace: " << std::endl;
    for (const auto &msg : traceVec) {
        std::cout << msg;
    }
    std::cout << "end trace" << std::endl;

    // if (collectMetric) {
    //     graphMetric.show();
    // }
}

const char *NaviResult::getErrorString(ErrorCode ec) {
    return CommonUtil::getErrorString(ec);
}

}
