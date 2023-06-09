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
#include "ha3/sql/ops/agg/AggKernel.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <memory>
#include <utility>

#include "alog/Logger.h"
#include "autil/HashAlgorithm.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/Pool.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/common/common.h"
#include "ha3/sql/data/TableData.h"
#include "ha3/sql/data/TableType.h"
#include "ha3/sql/ops/agg/AggGlobal.h"
#include "ha3/sql/ops/agg/AggLocal.h"
#include "ha3/sql/ops/agg/AggNormal.h"
#include "ha3/sql/ops/calc/CalcTable.h"
#include "ha3/sql/ops/util/KernelUtil.h"
#include "ha3/sql/proto/SqlSearchInfoCollector.h"
#include "ha3/sql/resource/MetaInfoResource.h"
#include "ha3/sql/resource/SqlBizResource.h"
#include "ha3/sql/resource/SqlQueryResource.h"
#include "kmonitor/client/MetricMacro.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/core/MutableMetric.h"
#include "matchdoc/flatbuffer/MatchDoc_generated.h"
#include "navi/common.h"
#include "navi/engine/CreatorRegistry.h"
#include "navi/engine/Data.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelConfigContext.h"
#include "navi/engine/N2OneKernel.h"
#include "navi/engine/Resource.h"
#include "navi/resource/GraphMemoryPoolResource.h" // IWYU pragma: keep
#include "table/Row.h"
#include "table/Table.h"
#include "table/TableUtil.h"

namespace navi {
class KernelInitContext;
} // namespace navi

namespace kmonitor {
class MetricsTags;
} // namespace kmonitor

using namespace std;
using namespace autil;
using namespace table;
using namespace matchdoc;
using namespace kmonitor;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, AggKernel);

class AggOpMetrics : public MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_GAUGE_MUTABLE_METRIC(_totalComputeTimes, "TotalComputeTimes");
        REGISTER_GAUGE_MUTABLE_METRIC(_outputCount, "OutputCount");
        REGISTER_LATENCY_MUTABLE_METRIC(_totalAggTime, "TotalAggTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_collectTime, "collectTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_outputAccTime, "outputAccTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_mergeTime, "mergeTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_outputResultTime, "outputResultTime");
        REGISTER_GAUGE_MUTABLE_METRIC(_aggPoolSize, "aggPoolSize");
        REGISTER_GAUGE_MUTABLE_METRIC(_totalInputCount, "TotalInputCount");
        return true;
    }
    void report(const kmonitor::MetricsTags *tags, AggInfo *aggInfo) {
        REPORT_MUTABLE_METRIC(_totalComputeTimes, aggInfo->totalcomputetimes());
        REPORT_MUTABLE_METRIC(_outputCount, aggInfo->totaloutputcount());
        REPORT_MUTABLE_METRIC(_totalAggTime, aggInfo->totalusetime() / 1000);
        if (aggInfo->collecttime() > 0) {
            REPORT_MUTABLE_METRIC(_collectTime, aggInfo->collecttime() / 1000);
        }
        if (aggInfo->outputacctime() > 0) {
            REPORT_MUTABLE_METRIC(_outputAccTime, aggInfo->outputacctime() / 1000);
        }
        if (aggInfo->mergetime() > 0) {
            REPORT_MUTABLE_METRIC(_mergeTime, aggInfo->mergetime() / 1000);
        }
        if (aggInfo->outputresulttime() > 0) {
            REPORT_MUTABLE_METRIC(_outputResultTime, aggInfo->outputresulttime() / 1000);
        }
        REPORT_MUTABLE_METRIC(_aggPoolSize, aggInfo->aggpoolsize());
        REPORT_MUTABLE_METRIC(_totalInputCount, aggInfo->totalinputcount());
    }

private:
    MutableMetric *_totalComputeTimes = nullptr;
    MutableMetric *_outputCount = nullptr;
    MutableMetric *_totalAggTime = nullptr;
    MutableMetric *_collectTime = nullptr;
    MutableMetric *_outputAccTime = nullptr;
    MutableMetric *_mergeTime = nullptr;
    MutableMetric *_outputResultTime = nullptr;
    MutableMetric *_aggPoolSize = nullptr;
    MutableMetric *_totalInputCount = nullptr;
};

AggKernel::AggKernel()
    : N2OneKernel("AggKernel")
    , _opId(-1) {}

std::vector<navi::StaticResourceBindInfo> AggKernel::dependResources() const {
    return {{SqlBizResource::RESOURCE_ID, true, BIND_RESOURCE_TO(_bizResource)},
            {SqlQueryResource::RESOURCE_ID, true, BIND_RESOURCE_TO(_queryResource)},
            {navi::GRAPH_MEMORY_POOL_RESOURCE_ID, true, BIND_RESOURCE_TO(_memoryPoolResource)},
            {navi::META_INFO_RESOURCE_ID, false, BIND_RESOURCE_TO(_metaInfoResource)}};
}

std::string AggKernel::inputPort() const {
    return "input0";
}

std::string AggKernel::inputType() const {
    return TableType::TYPE_ID;
}

std::string AggKernel::outputPort() const {
    return "output0";
}

std::string AggKernel::outputType() const {
    return TableType::TYPE_ID;
}

bool AggKernel::config(navi::KernelConfigContext &ctx) {
    ctx.Jsonize(AGG_OUTPUT_FIELD_ATTRIBUTE, _outputFields);
    ctx.Jsonize(AGG_OUTPUT_FIELD_TYPE_ATTRIBUTE, _outputFieldsType);
    ctx.Jsonize(AGG_SCOPE_ATTRIBUTE, _scope);
    ctx.Jsonize(AGG_FUNCTION_ATTRIBUTE, _aggFuncDesc, _aggFuncDesc);
    ctx.Jsonize(AGG_GROUP_BY_KEY_ATTRIBUTE, _groupKeyVec, _groupKeyVec);
    ctx.Jsonize(IQUAN_OP_ID, _opId, _opId);
    ctx.Jsonize("reuse_inputs", _reuseInputs, _reuseInputs);
    std::map<std::string, std::map<std::string, std::string>> hintsMap;
    ctx.Jsonize("hints", hintsMap, hintsMap);
    patchHintInfo(hintsMap);
    KernelUtil::stripName(_outputFields);
    KernelUtil::stripName(_groupKeyVec);
    return true;
}

navi::ErrorCode AggKernel::init(navi::KernelInitContext &initContext) {
    _aggFuncManager = _bizResource->getAggFuncManager();
    KERNEL_REQUIRES(_aggFuncManager, "get agg Func Manager failed");

    _queryMetricsReporter = _queryResource->getQueryMetricsReporter();
    CalcInitParam calcInitParam(_outputFields, _outputFieldsType);
    CalcResource calcResource;
    calcResource.memoryPoolResource = _memoryPoolResource;
    calcResource.tracer = _queryResource->getTracer();
    calcResource.cavaAllocator = _queryResource->getSuezCavaAllocator();
    calcResource.cavaPluginManager = _bizResource->getCavaPluginManager();
    calcResource.funcInterfaceCreator = _bizResource->getFunctionInterfaceCreator();
    calcResource.metricsReporter = _queryMetricsReporter;
    _calcTable.reset(new CalcTable(calcInitParam, std::move(calcResource)));

    if (_scope == "PARTIAL") {
        _aggBase.reset(new AggLocal(_memoryPoolResource, _aggHints));
    } else if (_scope == "FINAL") {
        _aggBase.reset(new AggGlobal(_memoryPoolResource, _aggHints));
    } else if (_scope == "NORMAL") {
        _aggBase.reset(new AggNormal(_memoryPoolResource, _aggHints));
    } else {
        SQL_LOG(ERROR, "scope type [%s] invalid", _scope.c_str());
        return navi::EC_INIT_GRAPH;
    }
    if (!_aggBase->init(_aggFuncManager, _groupKeyVec, _outputFields, _aggFuncDesc)) {
        SQL_LOG(ERROR, "agg init failed");
        return navi::EC_INIT_GRAPH;
    }

    _sqlSearchInfoCollector = _metaInfoResource ? _metaInfoResource->getOverwriteInfoCollector() : nullptr;
    _aggInfo.set_kernelname(getKernelName());
    _aggInfo.set_nodename(getNodeName());
    if (_opId < 0) {
        string keyStr = getKernelName();
        _aggInfo.set_hashkey(autil::HashAlgorithm::hashString(keyStr.c_str(), keyStr.size(), 1));
    } else {
        _aggInfo.set_hashkey(_opId);
    }
    return navi::EC_NONE;
}

navi::ErrorCode AggKernel::collect(const navi::DataPtr &data) {
    incComputeTime();
    uint64_t beginTime = TimeUtility::currentTime();

    auto inputTable = KernelUtil::getTable(data, _memoryPoolResource, false);
    if (!inputTable) {
        SQL_LOG(ERROR, "invalid input table");
        return navi::EC_ABORT;
    }
    incTotalInputCount(inputTable->getRowCount());
    if (!_aggBase->compute(inputTable)) {
        SQL_LOG(ERROR, "agg compute failed");
        return navi::EC_ABORT;
    }
    incTotalTime(TimeUtility::currentTime() - beginTime);
    return navi::EC_NONE;
}

navi::ErrorCode AggKernel::finalize(navi::DataPtr &data) {
    uint64_t beginTime = TimeUtility::currentTime();
    if (!_aggBase->finalize()) {
        SQL_LOG(ERROR, "agg finalize failed");
        return navi::EC_ABORT;
    }
    TablePtr table = _aggBase->getTable();
    if (table == nullptr) {
        SQL_LOG(ERROR, "get output table failed");
        return navi::EC_ABORT;
    }
    SQL_LOG(TRACE1, "aggregate table row count [%lu]", table->getRowCount());
    if (!_calcTable->projectTable(table)) {
        SQL_LOG(WARN, "project table [%s] failed.", TableUtil::toString(table, 5).c_str());
        return navi::EC_ABORT;
    }
    incOutputCount(table->getRowCount());
    TableDataPtr tableData(new TableData(table));
    data = tableData;
    incTotalTime(TimeUtility::currentTime() - beginTime);

    uint64_t collectTime, outputAccTime, mergeTime, outputResultTime, aggPoolSize;
    _aggBase->getStatistics(collectTime, outputAccTime, mergeTime, outputResultTime, aggPoolSize);
    _aggInfo.set_collecttime(collectTime);
    _aggInfo.set_outputacctime(outputAccTime);
    _aggInfo.set_mergetime(mergeTime);
    _aggInfo.set_outputresulttime(outputResultTime);
    _aggInfo.set_aggpoolsize(aggPoolSize);

    reportMetrics();

    if (_sqlSearchInfoCollector) {
        _sqlSearchInfoCollector->overwriteAggInfo(_aggInfo);
    }
    SQL_LOG(TRACE1, "agg output table: [%s]", TableUtil::toString(table, 10).c_str());
    SQL_LOG(DEBUG, "agg info: [%s]", _aggInfo.ShortDebugString().c_str());
    return navi::EC_NONE;
}

void AggKernel::patchHintInfo(const map<string, map<string, string>> &hintsMap) {
    const auto &mapIter = hintsMap.find(SQL_AGG_HINT);
    if (mapIter == hintsMap.end()) {
        return;
    }
    const map<string, string> &hints = mapIter->second;
    if (hints.empty()) {
        return;
    }
    auto iter = hints.find("groupKeyLimit");
    if (iter != hints.end()) {
        size_t keyLimit = 0;
        StringUtil::fromString(iter->second, keyLimit);
        if (keyLimit > 0) {
            _aggHints.groupKeyLimit = keyLimit;
        }
    }
    iter = hints.find("stopExceedLimit");
    if (iter != hints.end()) {
        bool stop = true;
        StringUtil::fromString(iter->second, stop);
        _aggHints.stopExceedLimit = stop;
    }
    iter = hints.find("memoryLimit");
    if (iter != hints.end()) {
        size_t memoryLimit = 0;
        StringUtil::fromString(iter->second, memoryLimit);
        _aggHints.memoryLimit = memoryLimit;
    }
    iter = hints.find("funcHint");
    if (iter != hints.end()) {
        _aggHints.funcHint = iter->second;
    }
}

void AggKernel::reportMetrics() {
    if (_queryMetricsReporter != nullptr) {
        string pathName = "sql.user.ops." + getKernelName();
        auto opMetricsReporter
            = _queryMetricsReporter->getSubReporter(pathName, {{{"scope", _scope}}});
        opMetricsReporter->report<AggOpMetrics, AggInfo>(nullptr, &_aggInfo);
    }
}

void AggKernel::incComputeTime() {
    _aggInfo.set_totalcomputetimes(_aggInfo.totalcomputetimes() + 1);
}

void AggKernel::incOutputCount(uint32_t count) {
    _aggInfo.set_totaloutputcount(_aggInfo.totaloutputcount() + count);
}

void AggKernel::incTotalTime(int64_t time) {
    _aggInfo.set_totalusetime(_aggInfo.totalusetime() + time);
}

void AggKernel::incTotalInputCount(size_t count) {
    _aggInfo.set_totalinputcount(_aggInfo.totalinputcount() + count);
}

REGISTER_KERNEL(AggKernel);

} // namespace sql
} // namespace isearch
