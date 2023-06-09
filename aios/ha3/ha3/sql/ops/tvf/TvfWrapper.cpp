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
#include "ha3/sql/ops/tvf/TvfWrapper.h"

#include "autil/TimeUtility.h"
#include "ha3/sql/resource/SqlBizResource.h"
#include "ha3/sql/resource/SqlQueryResource.h"
#include "ha3/sql/resource/MetaInfoResource.h"
#include "ha3/sql/ops/tvf/TvfFuncCreatorR.h"
#include "ha3/sql/ops/util/KernelUtil.h"
#include "ha3/sql/ops/tvf/TvfFuncManager.h"
#include "ha3/sql/proto/SqlSearchInfoCollector.h"
#include "ha3/sql/proto/SqlSearchInfo.pb.h"
#include "navi/resource/GraphMemoryPoolResource.h" // IWYU pragma: keep
#include "kmonitor/client/MetricMacro.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/core/MutableMetric.h"
#include "table/TableUtil.h"

using namespace std;
using namespace autil;
using namespace navi;
using namespace kmonitor;
using namespace table;

namespace isearch {
namespace sql {

AUTIL_LOG_SETUP(sql, TvfWrapper);

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
    , reserveMaxCount(0)
{
}

bool TvfInitParam::initFromJson(KernelConfigContext &ctx) {
    ctx.Jsonize("invocation", invocation, invocation);
    ctx.Jsonize("output_fields", outputFields, outputFields);
    ctx.Jsonize("output_fields_type", outputFieldsType, outputFieldsType);
    ctx.Jsonize("reuse_inputs", reuseInputs, reuseInputs);
    ctx.Jsonize(IQUAN_OP_ID, opId, opId);
    ctx.Jsonize("reserve_max_count", reserveMaxCount, reserveMaxCount);
    string str;
    ctx.Jsonize("scope", str, str);
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

TvfWrapper::TvfWrapper(const TvfInitParam &initParam,
                       SqlBizResource *bizResource,
                       SqlQueryResource *queryResource,
                       MetaInfoResource *metaInfoResource,
                       navi::GraphMemoryPoolResource *memoryPoolResource)
    : _initParam(initParam)
    , _bizResource(bizResource)
    , _queryResource(queryResource)
    , _metaInfoResource(metaInfoResource)
    , _memoryPoolResource(memoryPoolResource)
    , _reuseTable(false)
{
}

TvfWrapper::~TvfWrapper() {
    reportMetrics();
}

bool TvfWrapper::init() {
    if (_metaInfoResource) {
        _sqlSearchInfoCollector = _metaInfoResource->getOverwriteInfoCollector();
    }
    _queryMetricsReporter = _queryResource->getQueryMetricsReporter();
    auto tvfFuncManager = _bizResource->getTvfFuncManager();
    if (!tvfFuncManager) {
        SQL_LOG(ERROR, "get tvf func manager failed");
        return false;
    }
    autil::mem_pool::Pool *pool = _queryResource->getPool();
    if (!pool) {
        SQL_LOG(ERROR, "get query pool failed");
        return false;
    }
    const std::string &tvfName = _initParam.invocation.tvfName;
    auto func = tvfFuncManager->createTvfFunction(tvfName);
    if (func == nullptr) {
        auto creatorMap = _queryResource->getTvfNameToCreator();
        if (creatorMap) {
            auto it = creatorMap->find(tvfName);
            if (it != creatorMap->end()) {
                func = it->second->createFunction(tvfName);
            }
        }
        if (func == nullptr) {
            SQL_LOG(ERROR, "create tvf funcition [%s] failed.", tvfName.c_str());
            return false;
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
        return false;
    }
    tvfContext.tvfResourceContainer = tvfFuncManager->getTvfResourceContainer();
    tvfContext.queryPool = _queryResource->getPool();
    tvfContext.poolResource = _memoryPoolResource;
    tvfContext.suezCavaAllocator = _queryResource->getSuezCavaAllocator();
    tvfContext.queryInfo = _bizResource->getQueryInfo();
    tvfContext.modelConfigMap = _queryResource->getModelConfigMap();
    tvfContext.querySession = _queryResource->getGigQuerySession();
    tvfContext.metricReporter = _queryResource->getQueryMetricsReporter();
    if (!_tvfFunc->init(tvfContext)) {
        SQL_LOG(ERROR, "init tvf function [%s] failed.", tvfName.c_str());
        return false;
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
    return true;
}

bool TvfWrapper::compute(table::TablePtr &inputTable, bool &isEof, table::TablePtr &outputTable) {
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
    if (_sqlSearchInfoCollector) {
        _sqlSearchInfoCollector->overwriteTvfInfo(_tvfInfo);
    }
    return true;
}

bool TvfWrapper::compute(table::TablePtr &table, bool &isEof) {
    table::TablePtr outputTable;
    bool ret = compute(table, isEof, outputTable);
    if (ret) {
        table = outputTable;
    }
    return ret;
}


bool TvfWrapper::checkOutputTable(const table::TablePtr &table) {
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

void TvfWrapper::reportMetrics() {
    if (_queryMetricsReporter != nullptr) {
        string pathName = "sql.user.ops." + _initParam.opName;
        auto opMetricsReporter = _queryMetricsReporter->getSubReporter(pathName, {});
        opMetricsReporter->report<TvfOpMetrics, TvfInfo>(nullptr, &_tvfInfo);
    }
}

void TvfWrapper::incTotalTime(int64_t time) {
    _tvfInfo.set_totalusetime(_tvfInfo.totalusetime() + time);
}

void TvfWrapper::incOutputCount(int64_t count) {
    _tvfInfo.set_totaloutputcount(_tvfInfo.totaloutputcount() + count);
}

void TvfWrapper::incComputeTimes() {
    _tvfInfo.set_totalcomputetimes(_tvfInfo.totalcomputetimes() + 1);
}

void TvfWrapper::incTotalInputCount(size_t count) {
    _tvfInfo.set_totalinputcount(_tvfInfo.totalinputcount() + count);
}

}  // namespace sql
}  // namespace isearch
