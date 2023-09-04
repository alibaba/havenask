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
#include "navi/proto/NaviStream.pb.h"

namespace navi {

RunGraphParams::RunGraphParams()
    : _threadLimit(DEFAULT_THREAD_LIMIT)
    , _timeoutMs(DEFAULT_TIMEOUT_MS)
    , _traceLevel(LOG_LEVEL_DISABLE)
    , _traceFormatPattern(DEFAULT_LOG_PATTERN)
    , _collectMetric(false)
    , _collectPerf(false)
    , _asyncIo(true)
    , _maxInline(DEFAULT_MAX_INLINE)
    , _resourceStage(RS_RUN_GRAPH_EXTERNAL)
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

void RunGraphParams::setTraceLevel(const std::string &traceLevelStr) {
    _traceLevel = getLevelByString(traceLevelStr);
}

std::string RunGraphParams::getTraceLevel() const {
    return LoggingEvent::levelStrByLevel(_traceLevel);
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
        traceApppender->addBtFilter(LogBtFilterParam(param.first, param.second));
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

bool RunGraphParams::overrideEdgeData(const OverrideData &data) {
    if (INVALID_GRAPH_ID == data.graphId) {
        return false;
    }
    std::string typeId;
    for (const auto &d : data.datas) {
        if (!d) {
            continue;
        }
        const auto &id = d->getTypeId();
        if (id.empty()) {
            NAVI_KERNEL_LOG(
                ERROR,
                "override data has empty type, data [%p] graphId [%d] "
                "outputNode [%s] outputPort [%s]",
                d.get(), data.graphId, data.outputNode.c_str(),
                data.outputPort.c_str());
            return false;
        }
        if (typeId.empty()) {
            typeId = id;
        } else if (typeId != id) {
            NAVI_KERNEL_LOG(ERROR,
                            "override data has diff type, [%s] and [%s], "
                            "graphId [%d] outputNode [%s] outputPort [%s]",
                            typeId.c_str(), id.c_str(), data.graphId,
                            data.outputNode.c_str(), data.outputPort.c_str());
            return false;
        }
    }
    _overrideDatas.emplace_back(data);
    return true;
}

const std::vector<OverrideData> &RunGraphParams::getOverrideDatas() const {
    return _overrideDatas;
}

bool RunGraphParams::addNamedData(const NamedData &data) {
    if (INVALID_GRAPH_ID == data.graphId) {
        return false;
    }
    auto ret = _namedDatas[data.graphId].emplace(data.name, data);
    return ret.second;
}

void RunGraphParams::clearNamedData() {
    _namedDatas.clear();
}

const SubNamedDataMap *RunGraphParams::getSubNamedDataMap(GraphId graphId) const {
    auto it = _namedDatas.find(graphId);
    if (_namedDatas.end() == it) {
        return nullptr;
    }
    return &it->second;
}

const NamedDataMap &RunGraphParams::getNamedDatas() const {
    return _namedDatas;
}

void RunGraphParams::setResourceStage(ResourceStage stage) {
    _resourceStage = stage;
}

ResourceStage RunGraphParams::getResourceStage() const {
    return _resourceStage;
}

bool RunGraphParams::fromProto(const RunParams &pbParams,
                               CreatorManager *creatorManager,
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
    params.setResourceStage(pbParams.resource_stage());
    deserializeTraceBtFilter(pbParams, params);

    if (!deserializeOverrideData(pbParams, creatorManager, params)) {
        NAVI_KERNEL_LOG(ERROR, "deserialize override data failed");
        return false;
    }
    if (!deserializeNamedData(pbParams, creatorManager, params)) {
        NAVI_KERNEL_LOG(ERROR, "deserialize override data failed");
        return false;
    }
    return true;
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

bool RunGraphParams::parseOverrideDataValues(const RunParams &pbParams,
                                             CreatorManager *creatorManager,
                                             std::unordered_map<int32_t, DataPtr> &dataMap) {
    const auto &overrideDataDef = pbParams.override_data();
    auto valueCount = overrideDataDef.datas_size();
    for (int32_t i = 0; i < valueCount; i++) {
        DataPtr data;
        const auto &valueDef = overrideDataDef.datas(i);
        const auto &dataStr = valueDef.data();
        if (!dataStr.empty()) {
            const auto &typeId = valueDef.type();
            auto dataType = creatorManager->getType(typeId);
            if (!dataType) {
                NAVI_KERNEL_LOG(ERROR, "deserialize override data failed, type [%s] not registered", typeId.c_str());
                return false;
            }
            if (!SerializeData::deserializeFromStr(NAVI_TLS_LOGGER, dataStr, dataType, data)) {
                NAVI_KERNEL_LOG(ERROR, "deserialize override data failed, type [%s]", typeId.c_str());
                return false;
            }
        }
        dataMap[i] = data;
    }
    return true;
}

bool RunGraphParams::deserializeOverrideData(const RunParams &pbParams,
                                             CreatorManager *creatorManager,
                                             RunGraphParams &params)
{
    const auto &overrideDataDef = pbParams.override_data();
    std::unordered_map<int32_t, DataPtr> dataMap;
    if (!parseOverrideDataValues(pbParams, creatorManager, dataMap)) {
        return false;
    }
    auto edgeCount = overrideDataDef.edge_overrides_size();
    for (int32_t i = 0; i < edgeCount; i++) {
        const auto &edgeOverride = overrideDataDef.edge_overrides(i);
        OverrideData overrideData;
        auto dataCount = edgeOverride.indexs_size();
        for (int32_t j = 0; j < dataCount; j++) {
            auto index = edgeOverride.indexs(j);
            auto it = dataMap.find(index);
            if (dataMap.end() == it) {
                NAVI_KERNEL_LOG(ERROR, "deserialize failed, data index [%d] not found", index);
                return false;
            }
            overrideData.datas.emplace_back(it->second);
        }
        overrideData.graphId = edgeOverride.graph_id();
        const auto &outputPortDef = edgeOverride.output_port();
        overrideData.outputNode = outputPortDef.node_name();
        overrideData.outputPort = outputPortDef.port_name();
        params.overrideEdgeData(overrideData);
    }
    return true;
}

bool RunGraphParams::toProto(const RunGraphParams &params,
                             GraphId matchGraphId,
                             CreatorManager *creatorManager,
                             NaviPartId partId,
                             int64_t timeoutMs,
                             RunParams &pbParams)
{
    auto sessionId = params.getSessionId();
    auto pbSessionId = pbParams.mutable_id();
    pbSessionId->set_instance(sessionId.instance);
    pbSessionId->set_query_id(sessionId.queryId);

    pbParams.set_thread_limit(params.getThreadLimit());
    pbParams.set_timeout_ms(timeoutMs);
    pbParams.set_task_queue_name(params.getTaskQueueName());
    pbParams.set_trace_level(params.getTraceLevel());
    pbParams.set_collect_metric(params.collectMetric());
    pbParams.set_collect_perf(params.collectPerf());
    pbParams.set_max_inline(params.getMaxInline());
    pbParams.set_resource_stage(params.getResourceStage());
    serializeTraceBtFilter(params, pbParams);

    if (!serializeOverrideData(params.getOverrideDatas(), matchGraphId,
                               creatorManager, partId, pbParams))
    {
        NAVI_KERNEL_LOG(ERROR, "serialize override data failed");
        return false;
    }
    if (!serializeNamedData(params.getNamedDatas(), matchGraphId, creatorManager,
                            pbParams))
    {
        NAVI_KERNEL_LOG(ERROR, "serialize named data failed");
        return false;
    }
    return true;
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
    GraphId matchGraphId,
    CreatorManager *creatorManager,
    NaviPartId partId,
    RunParams &pbParams)
{
    auto &pbOverrideData = *pbParams.mutable_override_data();
    std::map<DataPtr, int32_t> dataMap;
    size_t totalSize = 0;
    for (const auto &overrideData : overrideEdgeDatas) {
        auto graphId = overrideData.graphId;
        if (graphId != matchGraphId) {
            continue;
        }
        auto overridePartId = overrideData.partId;
        if (INVALID_NAVI_PART_ID != overridePartId && partId != overridePartId) {
            continue;
        }
        auto &edgeOverrides = *pbOverrideData.add_edge_overrides();
        for (const auto &data : overrideData.datas) {
            int32_t index = 0;
            auto it = dataMap.find(data);
            if (dataMap.end() != it) {
                edgeOverrides.add_indexs(it->second);
                continue;
            } else {
                index = pbOverrideData.datas_size();
                edgeOverrides.add_indexs(index);
            }
            dataMap[data] = index;
            auto dataValueDef = pbOverrideData.add_datas();
            if (data) {
                const auto &typeId = data->getTypeId();
                auto dataType = creatorManager->getType(typeId);
                if (!dataType) {
                    NAVI_KERNEL_LOG(ERROR,
                                    "serialize override data failed, type "
                                    "[%s] not registered, graphId [%d] "
                                    "outputNode [%s] outputPort [%s]",
                                    typeId.c_str(),
                                    graphId,
                                    overrideData.outputNode.c_str(),
                                    overrideData.outputPort.c_str());
                    return false;
                }
                auto mutableStr = dataValueDef->mutable_data();
                if (!SerializeData::serializeToStr(NAVI_TLS_LOGGER, dataType,
                                                   data, *mutableStr))
                {
                    NAVI_KERNEL_LOG(ERROR,
                                    "serialize override data failed, type [%s] data [%p] "
                                    "graphId [%d] outputNode [%s] outputPort [%s]",
                                    typeId.c_str(),
                                    data.get(),
                                    graphId,
                                    overrideData.outputNode.c_str(),
                                    overrideData.outputPort.c_str());
                    return false;
                }
                totalSize += mutableStr->size();
                if (totalSize > 512 * 1024 * 1024) {
                    NAVI_KERNEL_LOG(ERROR, "override data too large [%lu]", totalSize);
                    return false;
                }
                dataValueDef->set_type(typeId);
            } else {
                dataValueDef->set_data("");
                dataValueDef->set_type("");
            }
        }
        edgeOverrides.set_graph_id(graphId);
        auto outputPortDef = edgeOverrides.mutable_output_port();
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

bool RunGraphParams::serializeNamedData(const NamedDataMap &namedDataMap,
                                        GraphId matchGraphId,
                                        CreatorManager *creatorManager,
                                        RunParams &pbParams)
{
    for (const auto &pair : namedDataMap) {
        auto graphId = pair.first;
        if (graphId != matchGraphId) {
            continue;
        }
        const auto &dataMap = pair.second;
        for (const auto &dataPair : dataMap) {
            const auto &namedData = dataPair.second;
            const auto &name = namedData.name;
            const auto &data = namedData.data;
            auto &pbNamedData = *pbParams.add_named_datas();
            pbNamedData.set_graph_id(graphId);
            pbNamedData.set_name(name);
            if (data) {
                const auto &typeId = data->getTypeId();
                auto dataType = creatorManager->getType(typeId);
                if (!dataType) {
                    NAVI_KERNEL_LOG(
                        ERROR,
                        "serialize named data failed, type [%s] not "
                        "registered, data [%p] graphId [%d] name [%s]",
                        typeId.c_str(), data.get(), graphId, name.c_str());
                    return false;
                }
                pbNamedData.set_type(typeId);
                auto &pbSerializeStr = *pbNamedData.mutable_data();
                if (!SerializeData::serializeToStr(NAVI_TLS_LOGGER, dataType,
                                                   data, pbSerializeStr))
                {
                    NAVI_KERNEL_LOG(ERROR,
                                    "serialize named data failed, type [%s] "
                                    "data [%p] graphId [%d] name [%s]",
                                    typeId.c_str(), data.get(), graphId,
                                    name.c_str());
                    return false;
                }
            }
        }
    }
    return true;
}

bool RunGraphParams::deserializeNamedData(const RunParams &pbParams,
                                          CreatorManager *creatorManager,
                                          RunGraphParams &params)
{
    auto namedDataCount = pbParams.named_datas_size();
    for (int32_t i = 0; i < namedDataCount; i++) {
        NamedData namedData;
        const auto &pbNamedData = pbParams.named_datas(i);
        const auto &typeId = pbNamedData.type();
        const auto &dataStr = pbNamedData.data();
        DataPtr data;
        if (!dataStr.empty()) {
            auto dataType = creatorManager->getType(typeId);
            if (!dataType) {
                NAVI_KERNEL_LOG(ERROR,
                                "deserialize named data failed, type [%s] not "
                                "registered, graphId [%d] name [%s]",
                                typeId.c_str(), pbNamedData.graph_id(),
                                pbNamedData.name().c_str());
                return false;
            }
            if (!SerializeData::deserializeFromStr(NAVI_TLS_LOGGER, dataStr, dataType, data)) {
                NAVI_KERNEL_LOG(ERROR,
                                "deserialize named data failed, type [%s] "
                                "graphId [%d] name [%s]",
                                typeId.c_str(), pbNamedData.graph_id(),
                                pbNamedData.name().c_str());
                return false;
            }
        }
        namedData.graphId = pbNamedData.graph_id();
        namedData.name = pbNamedData.name();
        namedData.data = std::move(data);
        if (!params.addNamedData(std::move(namedData))) {
            NAVI_KERNEL_LOG(ERROR,
                            "named data name [%s] conflict, graphId [%d]",
                            namedData.name.c_str(), namedData.graphId);
            return false;
        }
    }
    return true;
}

}
