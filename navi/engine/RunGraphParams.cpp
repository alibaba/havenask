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
#include "navi/engine/RunGraphParams.h"
#include "navi/engine/GraphParam.h"
#include "navi/engine/SerializeData.h"
#include "navi/log/TraceAppender.h"
#include "navi/proto/NaviRpc.pb.h"
#include "navi/proto/NaviStream.pb.h"

namespace navi {

RunGraphParams::RunGraphParams()
    : _threadLimit(DEFAULT_THREAD_LIMIT)
    , _timeoutMs(DEFAULT_TIMEOUT_MS)
    , _traceFormatPattern(DEFAULT_LOG_PATTERN)
    , _collectMetric(false)
    , _collectPerf(false)
    , _asyncIo(true)
    , _maxInline(3)
    , _singlePoolUsageLimit(0)
{
}

RunGraphParams::~RunGraphParams() {
}

void RunGraphParams::setSessionId(SessionId id) {
    _id = id;
}

SessionId RunGraphParams::getSessionId() const {
    return _id;
}

void RunGraphParams::setThreadLimit(uint32_t limit) {
    _threadLimit = std::max(1u, limit);
}

uint32_t RunGraphParams::getThreadLimit() const {
    return _threadLimit;
}

void RunGraphParams::setTimeoutMs(int64_t timeout) {
    _timeoutMs = timeout;
}

int64_t RunGraphParams::getTimeoutMs() const {
    return _timeoutMs;
}

void RunGraphParams::setTaskQueueName(const std::string &taskQueueName) {
    _taskQueueName = taskQueueName;
}

const std::string &RunGraphParams::getTaskQueueName() const {
    return _taskQueueName;
}

void RunGraphParams::setTraceLevel(const std::string &traceLevel) {
    _traceLevel = traceLevel;
}

const std::string &RunGraphParams::getTraceLevel() const {
    return _traceLevel;
}

void RunGraphParams::setTraceFormatPattern(const std::string &pattern) {
    _traceFormatPattern = pattern;
}

const std::string &RunGraphParams::getTraceFormatPattern() const {
    return _traceFormatPattern;
}

void RunGraphParams::addTraceBtFilter(const std::string &file, int line) {
    _traceBtFilterParams.emplace_back(file, line);
}

void RunGraphParams::fillFilterParam(TraceAppender *traceApppender) {
    for (const auto &param : _traceBtFilterParams) {
        traceApppender->addBtFilter(TraceBtFilterParam(param.first, param.second));
    }
}

const std::vector<std::pair<std::string, int>> &RunGraphParams::getTraceBtFilter() const {
    return _traceBtFilterParams;
}

void RunGraphParams::setCollectMetric(bool collect) {
    _collectMetric = collect;
}

bool RunGraphParams::collectMetric() const {
    return _collectMetric;
}

void RunGraphParams::setCollectPerf(bool collect) {
    _collectPerf = collect;
}

bool RunGraphParams::collectPerf() const {
    return _collectPerf;
}

void RunGraphParams::setAsyncIo(bool async) {
    _asyncIo = async;
}

bool RunGraphParams::asyncIo() const {
    return _asyncIo;
}

int32_t RunGraphParams::getMaxInline() const {
    return _maxInline;
}

void RunGraphParams::setMaxInline(int32_t maxInline) {
    _maxInline = maxInline;
}

void RunGraphParams::setSinglePoolUsageLimit(uint64_t limit) {
    _singlePoolUsageLimit = limit;
}

uint64_t RunGraphParams::getSinglePoolUsageLimit() const {
    return _singlePoolUsageLimit;
}

void RunGraphParams::overrideEdgeData(const OverrideData &data) {
    _overrideDatas.emplace_back(data);
}

const std::vector<OverrideData> &RunGraphParams::getOverrideDatas() const {
    return _overrideDatas;
}

bool RunGraphParams::fromProto(const RunParams &pbParams,
                               CreatorManager *creatorManager,
                               const PoolPtr &pool,
                               RunGraphParams &params)
{
    NAVI_KERNEL_LOG(DEBUG, "pb run params: %s", pbParams.DebugString().c_str());
    SessionId sessionId;
    auto pbSessionId = pbParams.id();
    sessionId.instance = pbSessionId.instance();
    sessionId.queryId = pbSessionId.query_id();
    params.setSessionId(sessionId);

    params.setTaskQueueName(pbParams.task_queue_name());
    params.setTraceLevel(pbParams.trace_level());
    params.setThreadLimit(pbParams.thread_limit());
    params.setTimeoutMs(pbParams.timeout_ms());
    params.setCollectMetric(pbParams.collect_metric());
    params.setCollectPerf(pbParams.collect_perf());
    params.setMaxInline(pbParams.max_inline());
    params.setSinglePoolUsageLimit(pbParams.single_pool_usage_limit());
    deserializeTraceBtFilter(pbParams, params);

    return deserializeOverrideData(pbParams, creatorManager, pool, params);
}

void RunGraphParams::deserializeTraceBtFilter(const RunParams &pbParams,
                                              RunGraphParams &params)
{
    auto filterCount = pbParams.trace_bt_filters_size();
    for (int32_t i = 0; i < filterCount; i++) {
        const auto &filter = pbParams.trace_bt_filters(i);
        params.addTraceBtFilter(filter.file(), filter.line());
    }
}

bool RunGraphParams::deserializeOverrideData(const RunParams &pbParams,
                                             CreatorManager *creatorManager,
                                             const PoolPtr &pool,
                                             RunGraphParams &params)
{
    auto overrideCount = pbParams.override_datas_size();
    for (int32_t i = 0; i < overrideCount; i++) {
        OverrideData overrideData;
        const auto &pbOverrideData = pbParams.override_datas(i);
        const auto &typeId = pbOverrideData.type();
        const Type *dataType = nullptr;
        auto dataCount = pbOverrideData.datas_size();
        for (int32_t index = 0; index < dataCount; index++) {
            const auto &dataStr = pbOverrideData.datas(index);
            DataPtr data;
            if (!dataStr.empty()) {
                if (!dataType) {
                    dataType = creatorManager->getType(typeId);
                    if (!dataType) {
                        NAVI_KERNEL_LOG(ERROR, "type [%s] not registered",
                                        typeId.c_str());
                        return false;
                    }
                }
                if (!SerializeData::deserializeFromStr(NAVI_TLS_LOGGER, dataStr,
                                                       dataType, pool.get(), pool, data))
                {
                    return false;
                }
            }
            overrideData.datas.emplace_back(data);
        }
        overrideData.graphId = pbOverrideData.graph_id();
        const auto &outputPortDef = pbOverrideData.output_port();
        overrideData.outputNode = outputPortDef.node_name();
        overrideData.outputPort = outputPortDef.port_name();
        params.overrideEdgeData(overrideData);
    }
    return true;
}

bool RunGraphParams::toProto(const RunGraphParams &params,
                             const std::vector<OverrideData> *forkOverrideEdgeDatas,
                             const std::set<GraphId> &graphIdSet,
                             CreatorManager *creatorManager,
                             NaviPartId partId,
                             RunParams &pbParams)
{
    auto sessionId = params.getSessionId();
    auto pbSessionId = pbParams.mutable_id();
    pbSessionId->set_instance(sessionId.instance);
    pbSessionId->set_query_id(sessionId.queryId);

    pbParams.set_thread_limit(params.getThreadLimit());
    pbParams.set_timeout_ms(params.getTimeoutMs());
    pbParams.set_task_queue_name(params.getTaskQueueName());
    pbParams.set_trace_level(params.getTraceLevel());
    pbParams.set_collect_metric(params.collectMetric());
    pbParams.set_collect_perf(params.collectPerf());
    pbParams.set_max_inline(params.getMaxInline());
    pbParams.set_single_pool_usage_limit(params.getSinglePoolUsageLimit());
    serializeTraceBtFilter(params, pbParams);

    return serializeOverrideData(
        params.getOverrideDatas(), forkOverrideEdgeDatas, graphIdSet, creatorManager, partId, pbParams);
}

void RunGraphParams::serializeTraceBtFilter(const RunGraphParams &params,
                                            RunParams &pbParams)
{
    const auto &traceBtFilter = params.getTraceBtFilter();
    for (const auto &pair : traceBtFilter) {
        auto filter = pbParams.add_trace_bt_filters();
        filter->set_file(pair.first);
        filter->set_line(pair.second);
    }
}

bool RunGraphParams::serializeOverrideData(
    const std::vector<OverrideData> &overrideEdgeDatas,
    const std::vector<OverrideData> *forkOverrideEdgeDatas,
    const std::set<GraphId> &graphIdSet,
    CreatorManager *creatorManager,
    NaviPartId partId,
    RunParams &pbParams)
{
    const auto *realVec = &overrideEdgeDatas;
    if (forkOverrideEdgeDatas) {
        realVec = forkOverrideEdgeDatas;
    }
    for (const auto &overrideData : *realVec) {
        auto graphId = overrideData.graphId;
        if (graphIdSet.end() == graphIdSet.find(graphId)) {
            continue;
        }
        auto overridePartId = overrideData.partId;
        if (INVALID_NAVI_PART_ID != overridePartId && partId != overridePartId) {
            continue;
        }
        auto &pbOverrideData = *pbParams.add_override_datas();
        const Type *dataType = nullptr;
        for (const auto &data : overrideData.datas) {
            auto &pbSerializeStr = *pbOverrideData.add_datas();
            if (data) {
                if (!dataType) {
                    const auto &typeId = data->getTypeId();
                    dataType = creatorManager->getType(typeId);
                    if (!dataType) {
                        NAVI_KERNEL_LOG(ERROR, "type [%s] not registered",
                                        typeId.c_str());
                        return false;
                    }
                    pbOverrideData.set_type(typeId);
                }
                if (!SerializeData::serializeToStr(NAVI_TLS_LOGGER, dataType,
                                                   data, pbSerializeStr))
                {
                    return false;
                }
            }
        }
        pbOverrideData.set_graph_id(graphId);
        auto outputPortDef = pbOverrideData.mutable_output_port();
        outputPortDef->set_node_name(overrideData.outputNode);
        outputPortDef->set_port_name(overrideData.outputPort);
    }
    return true;
}

void RunGraphParams::setQuerySession(std::shared_ptr<multi_call::QuerySession> querySession) {
    _querySession = std::move(querySession);
}

const std::shared_ptr<multi_call::QuerySession> &RunGraphParams::getQuerySession() const {
    return _querySession;
}

}
