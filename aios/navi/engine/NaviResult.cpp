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
#include "navi/engine/GraphResult.h"
#include "navi/util/CommonUtil.h"

namespace navi {

NaviResult::NaviResult(const NaviLoggerPtr &logger)
    : _logger(this, "navi result", logger)
{
}

bool NaviResult::hasError() const {
    return EC_NONE != getErrorCode();
}

ErrorCode NaviResult::getErrorCode() const {
    if (!error) {
        return EC_NONE;
    }
    return error->ec;
}

std::string NaviResult::getErrorMessage() const {
    if (!error) {
        return std::string();
    }
    if (!error->errorEvent) {
        return std::string();
    }
    return error->errorEvent->message;
}

std::string NaviResult::getErrorPrefix() const {
    if (!error) {
        return std::string();
    }
    if (!error->errorEvent) {
        return std::string();
    }
    return error->errorEvent->prefix;
}

std::string NaviResult::getErrorBt() const {
    if (!error) {
        return std::string();
    }
    if (!error->errorEvent) {
        return std::string();
    }
    return error->errorEvent->bt;
}

LoggingEventPtr NaviResult::getErrorEvent() const {
    if (!error) {
        return nullptr;
    }
    return error->errorEvent;
}

void NaviResult::collectTrace(std::vector<std::string> &traceVec) const {
    NaviLoggerScope scope(_logger);
    TraceCollector collector;
    SymbolTableDef symbolTableDef;
    _graphResult->collectTrace(collector, symbolTableDef);
    collector.format(traceVec);
}

std::shared_ptr<GraphVisDef> NaviResult::getVisProto() const {
    NaviLoggerScope scope(_logger);
    return _graphResult->getVisProto();
}

std::shared_ptr<multi_call::GigStreamRpcInfoMap> NaviResult::getRpcInfoMap() {
    NaviLoggerScope scope(_logger);
    return _graphResult->getRpcInfoMap();
}

const char *NaviResult::getErrorString(ErrorCode ec) {
    return CommonUtil::getErrorString(ec);
}

}
