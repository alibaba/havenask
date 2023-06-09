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
#ifndef NAVI_NAVIRESULT_H
#define NAVI_NAVIRESULT_H

#include "navi/log/TraceCollector.h"
#include "navi/engine/GraphMetric.h"
#include "aios/network/gig/multi_call/stream/GigStreamRpcInfo.h"
#include "autil/DataBuffer.h"
#include <list>

namespace navi {

class GraphMetric;
class GraphVisDef;

class NaviResult
{
public:
    NaviResult();
    ~NaviResult();
private:
    NaviResult(const NaviResult &);
    NaviResult &operator=(const NaviResult &);
public:
    void setSessionId(SessionId id_);
    GraphMetric *getGraphMetric();
    void setCollectMetric(bool collect);
    void setTraceFormatPattern(const std::string &pattern);
public:
    void merge(TraceCollector &collector);
    void merge(NaviResult &result);
    void appendRpcInfo(const multi_call::GigStreamRpcInfoKey &key, multi_call::GigStreamRpcInfo info);
    void end();
    void show() const;
    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);
    void serializeToString(std::string &data, autil::mem_pool::Pool *pool) const;
    void deserializeFromString(const std::string &data, autil::mem_pool::Pool *pool);
public:
    static const char *getErrorString(ErrorCode ec);
public:
    GraphMetricTime getGraphMetricTime() const;
    multi_call::GigStreamRpcInfoMap stealRpcInfoMap() const;
    void setRpcInfoMap(multi_call::GigStreamRpcInfoMap rpcInfoMap_);
    std::shared_ptr<GraphVisDef> getGraphVisData() const;
    void fillGraphSummary(GraphVisDef *vis) const;
public:
    SessionId id;
    ErrorCode ec;
    LoggingEvent errorEvent;
    std::string errorBackTrace;
    TraceCollector traceCollector;
    GraphMetric graphMetric;
    bool collectMetric;
private:
    mutable autil::ThreadMutex _mergeLock;
    multi_call::GigStreamRpcInfoMap rpcInfoMap;
    GraphMetricTime _graphMetricTime;
};

NAVI_TYPEDEF_PTR(NaviResult);

}

#endif //NAVI_NAVIRESULT_H
