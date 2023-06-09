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
#ifndef NAVI_RUNGRAPHPARAMS_H
#define NAVI_RUNGRAPHPARAMS_H

#include "navi/common.h"
#include "navi/engine/Data.h"
#include <unordered_map>
#include <set>

namespace multi_call {
class QuerySession;
} // namespace multi_call

namespace navi {

class RunParams;
class CreatorManager;
class TraceAppender;

struct OverrideData {
public:
    OverrideData()
        : graphId(INVALID_GRAPH_ID)
        , partId(INVALID_NAVI_PART_ID)
    {
    }
public:
    GraphId graphId;
    NaviPartId partId;
    std::string outputNode;
    std::string outputPort;
    std::vector<DataPtr> datas;
};

class RunGraphParams
{
public:
    RunGraphParams();
    ~RunGraphParams();
public:
    void setSessionId(SessionId id);
    SessionId getSessionId() const;
    void setThreadLimit(uint32_t limit);
    uint32_t getThreadLimit() const;
    void setTimeoutMs(int64_t timeout);
    int64_t getTimeoutMs() const;
    void setTaskQueueName(const std::string &taskQueueName);
    const std::string &getTaskQueueName() const;
    void setTraceLevel(const std::string &traceLevel);
    const std::string &getTraceLevel() const;
    void setTraceFormatPattern(const std::string &pattern);
    const std::string &getTraceFormatPattern() const;
    void addTraceBtFilter(const std::string &file, int line);
    const std::vector<std::pair<std::string, int>> &getTraceBtFilter() const;
    void fillFilterParam(TraceAppender *traceApppender);
    void setCollectMetric(bool collect);
    bool collectMetric() const;
    void setCollectPerf(bool collect);
    bool collectPerf() const;
    void setAsyncIo(bool async);
    bool asyncIo() const;
    int32_t getMaxInline() const;
    void setMaxInline(int32_t maxInline);
    void setSinglePoolUsageLimit(uint64_t limit);
    uint64_t getSinglePoolUsageLimit() const;
    void overrideEdgeData(const OverrideData &data);
    const std::vector<OverrideData> &getOverrideDatas() const;
    void setQuerySession(std::shared_ptr<multi_call::QuerySession> querySession);
    const std::shared_ptr<multi_call::QuerySession> &getQuerySession() const;
public:
    static bool fromProto(const RunParams &pbParams,
                          CreatorManager *creatorManager,
                          const PoolPtr &pool,
                          RunGraphParams &params);
    static bool toProto(const RunGraphParams &params,
                        const std::vector<OverrideData> *forkOverrideEdgeDatas,
                        const std::set<GraphId> &graphIdSet,
                        CreatorManager *creatorManager,
                        NaviPartId partId,
                        RunParams &pbParams);
private:
    static void deserializeTraceBtFilter(const RunParams &pbParams,
                                         RunGraphParams &params);
    static void serializeTraceBtFilter(const RunGraphParams &params,
                                       RunParams &pbParams);
    static bool deserializeOverrideData(const RunParams &pbParams,
                                        CreatorManager *creatorManager,
                                        const PoolPtr &pool,
                                        RunGraphParams &params);
    static bool serializeOverrideData(
            const std::vector<OverrideData> &overrideEdgeDatas,
            const std::vector<OverrideData> *forkOverrideEdgeDatas,
            const std::set<GraphId> &graphIdSet,
            CreatorManager *creatorManager,
            NaviPartId partId,
            RunParams &pbParams);
private:
    SessionId _id;
    uint32_t _threadLimit;
    int64_t _timeoutMs;
    std::string _taskQueueName;
    std::string _traceLevel;
    std::string _traceFormatPattern;
    std::vector<std::pair<std::string, int>> _traceBtFilterParams;
    bool _collectMetric : 1;
    bool _collectPerf : 1;
    bool _asyncIo : 1;
    int32_t _maxInline;
    uint64_t _singlePoolUsageLimit; // Bytes
    std::vector<OverrideData> _overrideDatas;
    std::shared_ptr<multi_call::QuerySession> _querySession;
};

}

#endif //NAVI_RUNGRAPHPARAMS_H
