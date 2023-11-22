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
#include "navi/engine/GraphResult.h"
#include "navi/engine/NaviHostInfo.h"
#include "navi/proto/NaviStream.pb.h"

namespace navi {

GraphResult::GraphResult() {
}

GraphResult::~GraphResult() {
    DELETE_AND_SET_NULL(_graphDef);
}

void GraphResult::init(const NaviLoggerPtr &logger,
                       const std::string &formatPattern,
                       const NaviSymbolTablePtr &naviSymbolTable,
                       const NaviHostInfo *hostInfo)
{
    _logger = logger;
    _formatPattern = formatPattern;
    _graphMetric.setNaviSymbolTable(naviSymbolTable);
    _hostInfo = hostInfo;
}

void GraphResult::setGraphDef(GraphDef *graphDef) {
    if (_graphDef) {
        delete _graphDef;
    }
    _graphDef = new GraphDef();
    _graphDef->CopyFrom(*graphDef);
}

void GraphResult::fillHostInfo(GraphResultDef &graphResultDef) {
    if (_hostInfo) {
        _hostInfo->fillHostInfo(graphResultDef.mutable_host_info());
    }
}

bool GraphResult::toGraphResultDef(bool partial, GraphResultDef &graphResultDef) {
    TraceCollector traceCollector;
    traceCollector.setFormatPattern(_formatPattern);
    _logger->collectTrace(traceCollector);
    if (partial) {
        if (!traceCollector.empty()) {
            graphResultDef.set_version(NAVI_VERSION);
            traceCollector.toProto(graphResultDef.mutable_traces(),
                                   &_stringHashTable);
            if (_stringHashTable.needToProto()) {
                _stringHashTable.toProto(graphResultDef.mutable_symbol_table());
            }
            return true;
        } else {
            return false;
        }
    } else {
        graphResultDef.set_version(NAVI_VERSION);
        if (!traceCollector.empty()) {
            traceCollector.toProto(graphResultDef.mutable_traces(),
                                   &_stringHashTable);
        }
        NaviError::fillSessionId(graphResultDef.mutable_session(),
                                 _logger->getLoggerId());
        if (_error) {
            _error->toProto(*graphResultDef.mutable_error());
        }
        if (_graphDef) {
            graphResultDef.mutable_graph()->Swap(_graphDef);
        }
        if (_graphMetric.needToProto()) {
            _graphMetric.toProto(graphResultDef.mutable_graph_metric(),
                                 &_stringHashTable);
        }
        if (_stringHashTable.needToProto()) {
            _stringHashTable.toProto(graphResultDef.mutable_symbol_table());
        }
        fillHostInfo(graphResultDef);
        return true;
    }
}

void GraphResult::doToProto(bool partial, NaviMessage &naviMessage) {
    GraphResultDef graphResultDef;
    if (!toGraphResultDef(partial, graphResultDef)) {
        return;
    }
    SerializedResultDef *thisResult = nullptr;
    if (hasParent() || partial) {
        thisResult = naviMessage.add_sub_result();
    } else {
        thisResult = naviMessage.mutable_navi_result();
        if (graphResultDef.has_error()) {
            thisResult->mutable_error()->CopyFrom(graphResultDef.error());
        }
    }
    thisResult->set_result_str(graphResultDef.SerializeAsString());
    thisResult->set_has_trace(graphResultDef.has_traces());
    markLocation(thisResult->mutable_location());
}

void GraphResult::markThisLocation(SingleResultLocationDef *singleLocation) const {
    singleLocation->set_type(NRT_NORMAL);
}

void GraphResult::doCollectTrace(TraceCollector &collector,
                                 SymbolTableDef &symbolTableDef)
{
    _logger->collectTrace(collector);
}

void GraphResult::doToVisProto(GraphVisNodeDef *visDef) {
    ResultLocationDef location;
    markLocation(&location);
    auto thisDef = getVisNode(location, visDef);
    if (!toGraphResultDef(false, *thisDef->mutable_result())) {
        return;
    }
    if (thisDef->mutable_result()->has_symbol_table()) {
        auto resultSymbolTableDef =
            thisDef->mutable_result()->mutable_symbol_table();
        visDef->mutable_symbol_table()->MergeFrom(*resultSymbolTableDef);
        thisDef->mutable_result()->clear_symbol_table();
    }
}

void GraphResult::setError(const NaviLoggerPtr &logger, ErrorCode ec) {
    updateError(makeError(logger, ec));
}

NaviErrorPtr GraphResult::makeError(const NaviLoggerPtr &logger, ErrorCode ec) const {
    if (EC_NONE == ec || !logger) {
        return nullptr;
    }
    auto errorPtr = std::make_shared<NaviError>();
    auto &error = *errorPtr;
    error.id = logger->getLoggerId();
    error.ec = ec;
    error.errorEvent = logger->firstErrorEvent();
    markLocation(&error.location);
    return errorPtr;
}

NaviErrorPtr GraphResult::updateError(const NaviErrorPtr &error) {
    autil::ScopedLock lock(_resultMutex);
    if (_error) {
        return _error;
    }
    _error = error;
    return _error;
}

NaviErrorPtr GraphResult::getError() const {
    autil::ScopedLock lock(_resultMutex);
    return _error;
}

NaviResultPtr GraphResult::createNaviResult() {
    autil::ScopedLock lock(_resultMutex);
    auto result = std::make_shared<NaviResult>(_logger);
    _graphMetric.fillResult(*result);
    result->id = _logger->getLoggerId();
    result->error = _error;
    return result;
}

std::shared_ptr<multi_call::GigStreamRpcInfoMap> GraphResult::getRpcInfoMap() {
    autil::ScopedLock lock(_resultMutex);
    if (_rpcInfoMap) {
        return _rpcInfoMap;
    }
    auto ret = std::make_shared<multi_call::GigStreamRpcInfoMap>();
    collectRpcInfo(*ret);
    _rpcInfoMap = ret;
    return _rpcInfoMap;
}

std::shared_ptr<GraphVisDef> GraphResult::getVisProto() {
    autil::ScopedLock lock(_resultMutex);
    if (_visProto) {
        return _visProto;
    }
    auto ret = std::make_shared<GraphVisDef>();
    auto node = ret->add_nodes();
    toVisProto(node);
    node->set_version(NAVI_VERSION);
    _visProto = ret;
    return _visProto;
}

}

