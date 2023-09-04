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
#include "sql/ops/tvf/TvfWrapperR.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <engine/NaviConfigContext.h>
#include <map>
#include <memory>
#include <set>
#include <utility>

#include "alog/Logger.h"
#include "autil/HashAlgorithm.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/Pool.h"
#include "kmonitor/client/MetricLevel.h"
#include "kmonitor/client/MetricMacro.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/core/MutableMetric.h"
#include "navi/builder/ResourceDefBuilder.h"
#include "navi/engine/KernelConfigContext.h"
#include "navi/engine/Resource.h"
#include "navi/proto/KernelDef.pb.h"
#include "sql/common/Log.h"
#include "sql/ops/tvf/TvfFuncCreatorR.h"
#include "sql/ops/tvf/TvfFuncFactoryR.h"
#include "sql/ops/util/KernelUtil.h"
#include "sql/proto/SqlSearchInfo.pb.h"
#include "sql/proto/SqlSearchInfoCollector.h"
#include "suez/turing/expression/cava/common/SuezCavaAllocator.h"
#include "table/TableUtil.h"

namespace kmonitor {
class MetricsTags;
} // namespace kmonitor
namespace navi {
class ResourceInitContext;
} // namespace navi

using namespace std;
using namespace autil;
using namespace navi;
using namespace kmonitor;
using namespace table;

namespace sql {

class TvfOpMetrics : public MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_GAUGE_MUTABLE_METRIC(_totalComputeTimes, "TotalComputeTimes");
        REGISTER_GAUGE_MUTABLE_METRIC(_totalOutputCount, "TotalOutputCount");
        REGISTER_LATENCY_MUTABLE_METRIC(_totalUsedTime, "TotalUsedTime");
        REGISTER_GAUGE_MUTABLE_METRIC(_totalInputCount, "TotalInputCount");
        return true;
    }
    void report(const kmonitor::MetricsTags *tags, TvfInfo *tvfInfo) {
        REPORT_MUTABLE_METRIC(_totalComputeTimes, tvfInfo->totalcomputetimes());
        REPORT_MUTABLE_METRIC(_totalOutputCount, tvfInfo->totaloutputcount());
        REPORT_MUTABLE_METRIC(_totalUsedTime, tvfInfo->totalusetime() / 1000);
        REPORT_MUTABLE_METRIC(_totalInputCount, tvfInfo->totalinputcount());
    }

private:
    MutableMetric *_totalComputeTimes = nullptr;
    MutableMetric *_totalOutputCount = nullptr;
    MutableMetric *_totalUsedTime = nullptr;
    MutableMetric *_totalInputCount = nullptr;
};

TvfInitParam::TvfInitParam()
    : scope(KernelScope::NORMAL)
    , opId(-1)
    , reserveMaxCount(0) {}

bool TvfInitParam::initFromJson(KernelConfigContext &ctx) {
    NAVI_JSONIZE(ctx, navi::NAVI_BUILDIN_ATTR_NODE, nodeName);
    NAVI_JSONIZE(ctx, navi::NAVI_BUILDIN_ATTR_KERNEL, opName);
    NAVI_JSONIZE(ctx, "invocation", invocation, invocation);
    NAVI_JSONIZE(ctx, "output_fields", outputFields, outputFields);
    NAVI_JSONIZE(ctx, "output_fields_type", outputFieldsType, outputFieldsType);
    NAVI_JSONIZE(ctx, "reuse_inputs", reuseInputs, reuseInputs);
    NAVI_JSONIZE(ctx, IQUAN_OP_ID, opId, opId);
    NAVI_JSONIZE(ctx, "reserve_max_count", reserveMaxCount, reserveMaxCount);
    string str;
    NAVI_JSONIZE(ctx, "scope", str, str);
    if (str == "FINAL") {
        scope = KernelScope::FINAL;
    } else if (str == "PARTIAL") {
        scope = KernelScope::PARTIAL;
    } else if (str == "NORMAL") {
        scope = KernelScope::NORMAL;
    } else {
        scope = KernelScope::UNKNOWN;
    }

    KernelUtil::stripName(outputFields);
    if (invocation.type != "TVF" || invocation.tvfParams.size() == 0) {
        return false;
    }
    return true;
}

const std::string TvfWrapperR::RESOURCE_ID = "tvf_wrapper_r";

TvfWrapperR::TvfWrapperR()
    : _reuseTable(false) {}

TvfWrapperR::~TvfWrapperR() {
    reportMetrics();
}

void TvfWrapperR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_KERNEL);
}

bool TvfWrapperR::config(navi::ResourceConfigContext &ctx) {
    if (!_initParam.initFromJson(ctx)) {
        return false;
    }
    return true;
}

navi::ErrorCode TvfWrapperR::init(navi::ResourceInitContext &ctx) {
    auto tvfFuncFactoryR = _tvfFuncFactoryR;
    if (!tvfFuncFactoryR) {
        SQL_LOG(ERROR, "get tvf func manager failed");
        return navi::EC_ABORT;
    }
    const std::string &tvfName = _initParam.invocation.tvfName;
    auto func = tvfFuncFactoryR->createTvfFunction(tvfName);
    if (func == nullptr) {
        const auto &creatorMap = tvfFuncFactoryR->getTvfNameToCreator();
        auto it = creatorMap.find(tvfName);
        if (it != creatorMap.end()) {
            func = it->second->createFunction(tvfName);
        }
        if (func == nullptr) {
            SQL_LOG(ERROR, "create tvf funcition [%s] failed.", tvfName.c_str());
            return navi::EC_ABORT;
        }
    }
    _tvfFunc.reset(func);
    _tvfFunc->setScope(_initParam.scope);
    TvfFuncInitContext tvfContext;
    tvfContext.outputFields = _initParam.outputFields;
    tvfContext.outputFieldsType = _initParam.outputFieldsType;
    vector<string> inputTables;
    if (!_initParam.invocation.parseStrParams(tvfContext.params, inputTables)) {
        SQL_LOG(ERROR, "parse tvf param [%s] error.", _initParam.invocation.tvfParams.c_str());
        return navi::EC_ABORT;
    }
    tvfContext.queryPool = _queryMemPoolR->getPool().get();
    tvfContext.poolResource = _graphMemoryPoolR;
    tvfContext.suezCavaAllocator = _suezCavaAllocatorR->getAllocator().get();
    tvfContext.queryInfo = &_ha3QueryInfoR->getQueryInfo();
    tvfContext.modelConfigMap = &_modelConfigMapR->getModelConfigMap();
    tvfContext.metricReporter = _queryMetricReporterR->getReporter().get();
    if (!_tvfFunc->init(tvfContext)) {
        SQL_LOG(ERROR, "init tvf function [%s] failed.", tvfName.c_str());
        return navi::EC_ABORT;
    }

    _tvfInfo.set_kernelname(_initParam.opName + "__" + _initParam.invocation.tvfName);
    _tvfInfo.set_nodename(_initParam.nodeName);
    if (_initParam.opId < 0) {
        string keyStr = _initParam.opName + "__" + _initParam.invocation.tvfName + "__"
                        + StringUtil::toString(_initParam.outputFields);
        _tvfInfo.set_hashkey(autil::HashAlgorithm::hashString(keyStr.c_str(), keyStr.size(), 1));
    } else {
        _tvfInfo.set_hashkey(_initParam.opId);
    }
    return navi::EC_NONE;
}

bool TvfWrapperR::compute(table::TablePtr &inputTable, bool &isEof, table::TablePtr &outputTable) {
    incComputeTimes();
    uint64_t beginTime = TimeUtility::currentTime();
    if (inputTable != nullptr) {
        incTotalInputCount(inputTable->getRowCount());
    }
    if (!_tvfFunc->compute(inputTable, isEof, outputTable)) {
        SQL_LOG(ERROR, "tvf [%s] compute failed.", _tvfFunc->getName().c_str());
        return false;
    }

    if (isEof && inputTable != nullptr && outputTable == nullptr) {
        SQL_LOG(ERROR, "tvf [%s] output is empty.", _tvfFunc->getName().c_str());
        return false;
    }

    if (outputTable && !checkOutputTable(outputTable)) {
        SQL_LOG(ERROR, "tvf [%s] output table illegal.", _tvfFunc->getName().c_str());
        return false;
    }

    if (outputTable) {
        uint64_t totalComputeTimes = _tvfInfo.totalcomputetimes();
        if (totalComputeTimes < 5) {
            SQL_LOG(TRACE1,
                    "tvf batch [%lu] output table: [%s]",
                    totalComputeTimes,
                    TableUtil::toString(outputTable, 10).c_str());
        }

        incOutputCount(outputTable->getRowCount());
        uint64_t endTime = TimeUtility::currentTime();
        incTotalTime(endTime - beginTime);
    } else {
        uint64_t endTime = TimeUtility::currentTime();
        incTotalTime(endTime - beginTime);
    }

    if (isEof) {
        SQL_LOG(DEBUG, "tvf info: [%s]", _tvfInfo.ShortDebugString().c_str());
    }
    _sqlSearchInfoCollectorR->getCollector()->overwriteTvfInfo(_tvfInfo);
    return true;
}

bool TvfWrapperR::compute(table::TablePtr &table, bool &isEof) {
    table::TablePtr outputTable;
    bool ret = compute(table, isEof, outputTable);
    if (ret) {
        table = outputTable;
    }
    return ret;
}

bool TvfWrapperR::checkOutputTable(const table::TablePtr &table) {
    set<string> nameSet;
    size_t colCount = table->getColumnCount();
    for (size_t i = 0; i < colCount; i++) {
        nameSet.insert(table->getColumnName(i));
    }
    for (const auto &field : _initParam.outputFields) {
        if (nameSet.count(field) != 1) {
            SQL_LOG(ERROR, "output field [%s] not found.", field.c_str());
            return false;
        }
    }
    return true;
}

void TvfWrapperR::reportMetrics() {
    if (_queryMetricReporterR) {
        string pathName = "sql.user.ops." + _initParam.opName;
        auto opMetricsReporter = _queryMetricReporterR->getReporter()->getSubReporter(pathName, {});
        opMetricsReporter->report<TvfOpMetrics, TvfInfo>(nullptr, &_tvfInfo);
    }
}

void TvfWrapperR::incTotalTime(int64_t time) {
    _tvfInfo.set_totalusetime(_tvfInfo.totalusetime() + time);
}

void TvfWrapperR::incOutputCount(int64_t count) {
    _tvfInfo.set_totaloutputcount(_tvfInfo.totaloutputcount() + count);
}

void TvfWrapperR::incComputeTimes() {
    _tvfInfo.set_totalcomputetimes(_tvfInfo.totalcomputetimes() + 1);
}

void TvfWrapperR::incTotalInputCount(size_t count) {
    _tvfInfo.set_totalinputcount(_tvfInfo.totalinputcount() + count);
}

REGISTER_RESOURCE(TvfWrapperR);

} // namespace sql
