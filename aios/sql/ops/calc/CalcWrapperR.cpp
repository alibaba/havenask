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
#include "sql/ops/calc/CalcWrapperR.h"

#include <cstddef>
#include <engine/NaviConfigContext.h>
#include <utility>

#include "autil/HashAlgorithm.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "kmonitor/client/MetricLevel.h"
#include "kmonitor/client/MetricMacro.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/core/MutableMetric.h"
#include "navi/builder/ResourceDefBuilder.h"
#include "navi/engine/Resource.h"
#include "navi/proto/KernelDef.pb.h"
#include "sql/common/Log.h"
#include "sql/common/common.h"
#include "sql/proto/SqlSearchInfo.pb.h"
#include "sql/proto/SqlSearchInfoCollector.h"
#include "table/TableUtil.h"

namespace indexlib {
namespace index {
class InvertedIndexReader;
} // namespace index
} // namespace indexlib
namespace kmonitor {
class MetricsTags;
} // namespace kmonitor
namespace navi {
class ResourceInitContext;
} // namespace navi
namespace suez {
namespace turing {
class IndexInfoHelper;
class MetaInfo;
} // namespace turing
} // namespace suez

using namespace std;
using namespace autil;
using namespace navi;
using namespace kmonitor;
using namespace table;

namespace sql {

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

CalcWrapperR::CalcWrapperR() {}

CalcWrapperR::~CalcWrapperR() {
    reportMetrics();
}

void CalcWrapperR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_KERNEL);
}

bool CalcWrapperR::config(navi::ResourceConfigContext &ctx) {
    NAVI_JSONIZE(ctx, IQUAN_OP_ID, _opId, _opId);
    std::string nodeName;
    std::string opName;
    NAVI_JSONIZE(ctx, navi::NAVI_BUILDIN_ATTR_NODE, nodeName);
    NAVI_JSONIZE(ctx, navi::NAVI_BUILDIN_ATTR_KERNEL, opName);
    _calcInfo.set_kernelname(opName);
    _calcInfo.set_nodename(nodeName);
    return true;
}

navi::ErrorCode CalcWrapperR::init(navi::ResourceInitContext &ctx) {
    if (_opId < 0) {
        string keyStr = _calcInfo.kernelname() + "__" + _calcInitParamR->conditionJson + "__"
                        + StringUtil::toString(_calcInitParamR->outputFields);
        _calcInfo.set_hashkey(autil::HashAlgorithm::hashString(keyStr.c_str(), keyStr.size(), 1));
    } else {
        _calcInfo.set_hashkey(_opId);
    }
    return navi::EC_NONE;
}

bool CalcWrapperR::compute(table::TablePtr &table, bool &isEof) {
    incComputeTimes();
    incTotalInputCount(table->getRowCount());
    uint64_t beginTime = TimeUtility::currentTime();

    if (!_calcTableR->calcTable(table)) {
        SQL_LOG(ERROR, "filter table error.");
        return false;
    }

    if (!table) {
        return false;
    }
    incOutputCount(table->getRowCount());

    uint64_t totalComputeTimes = _calcInfo.totalcomputetimes();
    if (totalComputeTimes < 5) {
        SQL_LOG(TRACE2,
                "calc batch [%lu] output table: [%s]",
                totalComputeTimes,
                TableUtil::toString(table, 10).c_str());
    }

    uint64_t endTime = TimeUtility::currentTime();
    incTotalTime(endTime - beginTime);

    if (isEof) {
        SQL_LOG(TRACE1, "calc info: [%s]", _calcInfo.ShortDebugString().c_str());
    }
    const auto &collector = _sqlSearchInfoCollectorR->getCollector();
    if (collector) {
        collector->overwriteCalcInfo(_calcInfo);
    }
    return true;
}

void CalcWrapperR::setReuseTable(bool reuse) {
    if (_calcTableR) {
        _calcTableR->setReuseTable(reuse);
    }
}

bool CalcWrapperR::isReuseTable() const {
    if (_calcTableR) {
        return _calcTableR->isReuseTable();
    }

    return false;
}

void CalcWrapperR::reportMetrics() {
    if (_calcTableR) {
        auto reporter = _calcTableR->getOpMetricReporter();
        if (reporter) {
            reporter->report<CalcOpMetrics, CalcInfo>(nullptr, &_calcInfo);
        }
    }
}

void CalcWrapperR::incTotalTime(int64_t time) {
    _calcInfo.set_totalusetime(_calcInfo.totalusetime() + time);
}

void CalcWrapperR::incOutputCount(int64_t count) {
    _calcInfo.set_totaloutputcount(_calcInfo.totaloutputcount() + count);
}

void CalcWrapperR::incComputeTimes() {
    _calcInfo.set_totalcomputetimes(_calcInfo.totalcomputetimes() + 1);
}

void CalcWrapperR::setMatchInfo(std::shared_ptr<suez::turing::MetaInfo> metaInfo,
                                const suez::turing::IndexInfoHelper *indexInfoHelper,
                                std::shared_ptr<indexlib::index::InvertedIndexReader> indexReader) {
    _calcTableR->setMatchInfo(std::move(metaInfo), indexInfoHelper, std::move(indexReader));
}

void CalcWrapperR::incTotalInputCount(size_t count) {
    _calcInfo.set_totalinputcount(_calcInfo.totalinputcount() + count);
}

REGISTER_RESOURCE(CalcWrapperR);

} // namespace sql
