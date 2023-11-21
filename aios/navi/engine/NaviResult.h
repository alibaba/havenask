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
#pragma once

#include "navi/common.h"
#include "navi/engine/NaviError.h"
#include "navi/log/NaviLogger.h"
#include "aios/network/gig/multi_call/stream/GigStreamRpcInfo.h"

namespace navi {

class GraphResult;

class NaviResult
{
public:
    NaviResult(const NaviLoggerPtr &logger);
public:
    bool hasError() const;
    ErrorCode getErrorCode() const;
    std::string getErrorMessage() const;
    std::string getErrorPrefix() const;
    std::string getErrorBt() const;
    LoggingEventPtr getErrorEvent() const;
    void collectTrace(std::vector<std::string> &traceVec) const;
    std::shared_ptr<GraphVisDef> getVisProto() const;
    std::shared_ptr<multi_call::GigStreamRpcInfoMap> getRpcInfoMap();
public:
    static const char *getErrorString(ErrorCode ec);
public:
    SessionId id;
    NaviErrorPtr error;
    int64_t beginTime = 0;
    int64_t endTime = 0;
    int64_t queueUs = 0;
    int64_t computeUs = 0;
private:
    friend class NaviUserResult;
private:
    DECLARE_LOGGER();
    std::shared_ptr<GraphResult> _graphResult;
};

NAVI_TYPEDEF_PTR(NaviResult);

}
