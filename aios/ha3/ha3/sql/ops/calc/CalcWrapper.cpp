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
#include "ha3/sql/ops/calc/CalcWrapper.h"

#include "autil/TimeUtility.h"
#include "ha3/sql/resource/SqlBizResource.h"
#include "ha3/sql/resource/SqlQueryResource.h"
#include "ha3/sql/resource/MetaInfoResource.h"
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
AUTIL_LOG_SETUP(sql, CalcWrapper);

class CalcOpMetrics : public MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_GAUGE_MUTABLE_METRIC(_totalComputeTimes, "TotalComputeTimes");
        REGISTER_GAUGE_MUTABLE_METRIC(_totalOutputCount, "TotalOutputCount");
        REGISTER_LATENCY_MUTABLE_METRIC(_totalUsedTime, "TotalUsedTime");
        REGISTER_GAUGE_MUTABLE_METRIC(_totalInputCount, "TotalInputCount");
        return true;
    }
    void report(const kmonitor::MetricsTags *tags, CalcInfo *calcInfo) {
        REPORT_MUTABLE_METRIC(_totalComputeTimes, calcInfo->totalcomputetimes());
        REPORT_MUTABLE_METRIC(_totalOutputCount, calcInfo->totaloutputcount());
        REPORT_MUTABLE_METRIC(_totalUsedTime, calcInfo->totalusetime() / 1000);
        REPORT_MUTABLE_METRIC(_totalInputCount, calcInfo->totalinputcount());
    }

private:
    MutableMetric *_totalComputeTimes = nullptr;
    MutableMetric *_totalOutputCount = nullptr;
    MutableMetric *_totalUsedTime = nullptr;
    MutableMetric *_totalInputCount = nullptr;
};


CalcWrapper::CalcWrapper(const CalcInitParam& param,
                         SqlBizResource *bizResource,
                         SqlQueryResource *queryResource,
                         MetaInfoResource *metaInfoResource,
                         navi::GraphMemoryPoolResource *memoryPoolResource)
    : _param(param)
    , _bizResource(bizResource)
    , _queryResource(queryResource)
    , _metaInfoResource(metaInfoResource)
    , _memoryPoolResource(memoryPoolResource)
{}

CalcWrapper::~CalcWrapper() {
    reportMetrics();
}

bool CalcWrapper::init() {
    if (_metaInfoResource) {
        _sqlSearchInfoCollector = _metaInfoResource->getOverwriteInfoCollector();
    }
    auto queryMetricsReporter = _queryResource->getQueryMetricsReporter();
    if (queryMetricsReporter != nullptr) {
        string pathName = "sql.user.ops." + _param.opName;
        _opMetricReporter = queryMetricsReporter->getSubReporter(pathName, {}).get();
    }
    CalcResource calcResource;
    calcResource.memoryPoolResource = _memoryPoolResource;
    calcResource.tracer = _queryResource->getTracer();
    calcResource.cavaAllocator = _queryResource->getSuezCavaAllocator();
    calcResource.cavaPluginManager = _bizResource->getCavaPluginManager();
    calcResource.funcInterfaceCreator = _bizResource->getFunctionInterfaceCreator();
    calcResource.metricsReporter = _opMetricReporter;
    _calcTablePtr.reset(new CalcTable(_param, std::move(calcResource)));
    if (!_calcTablePtr->init()) {
        return false;
    }
    _calcInfo.set_kernelname(_param.opName);
    _calcInfo.set_nodename(_param.nodeName);
    if (_param.opId < 0) {
        string keyStr = _param.opName + "__" + _param.conditionJson + "__" +
            StringUtil::toString(_param.outputFields);
        _calcInfo.set_hashkey(autil::HashAlgorithm::hashString(keyStr.c_str(), keyStr.size(), 1));
    } else {
        _calcInfo.set_hashkey(_param.opId);
    }
    return true;
}

bool CalcWrapper::compute(table::TablePtr &table, bool &isEof) {
    incComputeTimes();
    incTotalInputCount(table->getRowCount());
    uint64_t beginTime = TimeUtility::currentTime();

    if (!_calcTablePtr->calcTable(table)) {
        SQL_LOG(ERROR, "filter table error.");
        return false;
    }

    if (!table) {
        return false;
    }
    incOutputCount(table->getRowCount());

    uint64_t totalComputeTimes = _calcInfo.totalcomputetimes();
    if (totalComputeTimes < 5) {
        SQL_LOG(TRACE1,
                "calc batch [%lu] output table: [%s]",
                totalComputeTimes,
                TableUtil::toString(table, 10).c_str());
    }

    uint64_t endTime = TimeUtility::currentTime();
    incTotalTime(endTime - beginTime);

    if (isEof) {
        SQL_LOG(DEBUG, "calc info: [%s]", _calcInfo.ShortDebugString().c_str());
    }
    if (_sqlSearchInfoCollector) {
        _sqlSearchInfoCollector->overwriteCalcInfo(_calcInfo);
    }
    return true;
}

void CalcWrapper::setReuseTable(bool reuse) {
    if (_calcTablePtr) {
        _calcTablePtr->setReuseTable(reuse);
    }
}

bool CalcWrapper::isReuseTable() const {
    if (_calcTablePtr) {
        return _calcTablePtr->isReuseTable();
    }

    return false;
}

void CalcWrapper::reportMetrics() {
    if (_opMetricReporter != nullptr) {
        _opMetricReporter->report<CalcOpMetrics, CalcInfo>(nullptr, &_calcInfo);
    }
}

void CalcWrapper::incTotalTime(int64_t time) {
    _calcInfo.set_totalusetime(_calcInfo.totalusetime() + time);
}

void CalcWrapper::incOutputCount(int64_t count) {
    _calcInfo.set_totaloutputcount(_calcInfo.totaloutputcount() + count);
}

void CalcWrapper::incComputeTimes() {
    _calcInfo.set_totalcomputetimes(_calcInfo.totalcomputetimes() + 1);
}

void CalcWrapper::setMatchInfo(std::shared_ptr<suez::turing::MetaInfo> metaInfo,
                               const suez::turing::IndexInfoHelper *indexInfoHelper,
                               std::shared_ptr<indexlib::index::InvertedIndexReader> indexReader) {
    _calcTablePtr->setMatchInfo(std::move(metaInfo), indexInfoHelper, std::move(indexReader));
}

void CalcWrapper::incTotalInputCount(size_t count) {
    _calcInfo.set_totalinputcount(_calcInfo.totalinputcount() + count);
}

}  // namespace sql
}  // namespace isearch
