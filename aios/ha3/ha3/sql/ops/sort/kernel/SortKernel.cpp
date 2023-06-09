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
#include "ha3/sql/ops/sort/SortKernel.h"

#include <algorithm>
#include <cstddef>
#include <memory>

#include "alog/Logger.h"
#include "autil/HashAlgorithm.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/Pool.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/common/common.h"
#include "ha3/sql/data/TableData.h"
#include "ha3/sql/data/TableType.h"
#include "ha3/sql/ops/util/KernelUtil.h"
#include "ha3/sql/proto/SqlSearchInfo.pb.h"
#include "ha3/sql/proto/SqlSearchInfoCollector.h"
#include "ha3/sql/resource/MetaInfoResource.h"
#include "ha3/sql/resource/SqlQueryResource.h"
#include "kmonitor/client/MetricMacro.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/core/MutableMetric.h"
#include "navi/builder/KernelDefBuilder.h"
#include "navi/common.h"
#include "navi/engine/CreatorRegistry.h"
#include "navi/engine/Data.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelComputeContext.h"
#include "navi/engine/KernelConfigContext.h"
#include "navi/engine/Resource.h"
#include "navi/resource/GraphMemoryPoolResource.h"
#include "table/ComboComparator.h"
#include "table/ComparatorCreator.h"
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
using namespace kmonitor;
using namespace table;

namespace isearch {
namespace sql {

AUTIL_LOG_SETUP(sql, SortKernel);

class SortOpMetrics : public MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_GAUGE_MUTABLE_METRIC(_totalComputeTimes, "TotalComputeTimes");
        REGISTER_LATENCY_MUTABLE_METRIC(_totalSortTime, "TotalSortTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_totalMergeTime, "TotalMergeTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_totalTopKTime, "TotalTopK");
        REGISTER_LATENCY_MUTABLE_METRIC(_totalCompactTime, "totalCompactTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_outputTime, "OutputTime");
        REGISTER_GAUGE_MUTABLE_METRIC(_totalInputCount, "TotalInputCount");
        return true;
    }
    void report(const kmonitor::MetricsTags *tags, SortInfo *sortInfo) {
        REPORT_MUTABLE_METRIC(_totalComputeTimes, sortInfo->totalcomputetimes());
        REPORT_MUTABLE_METRIC(_totalSortTime, sortInfo->totalusetime() / 1000);
        REPORT_MUTABLE_METRIC(_totalMergeTime, sortInfo->totalmergetime() / 1000);
        REPORT_MUTABLE_METRIC(_totalTopKTime, sortInfo->totaltopktime() / 1000);
        REPORT_MUTABLE_METRIC(_totalCompactTime, sortInfo->totalcompacttime() / 1000);
        REPORT_MUTABLE_METRIC(_outputTime, sortInfo->totaloutputtime() / 1000);
        REPORT_MUTABLE_METRIC(_totalInputCount, sortInfo->totalinputcount());
    }

private:
    MutableMetric *_totalComputeTimes = nullptr;
    MutableMetric *_totalSortTime = nullptr;
    MutableMetric *_totalMergeTime = nullptr;
    MutableMetric *_totalTopKTime = nullptr;
    MutableMetric *_totalCompactTime = nullptr;
    MutableMetric *_outputTime = nullptr;
    MutableMetric *_totalInputCount = nullptr;
};

SortKernel::SortKernel()
    : _queryMetricsReporter(nullptr)
    , _sqlSearchInfoCollector(nullptr)
    , _opId(-1) {}

SortKernel::~SortKernel() {
    reportMetrics();
}

// kernel def, see: navi/sql/executor/proto/KernelDef.proto
void SortKernel::def(navi::KernelDefBuilder &builder) const {
    builder.name("SortKernel")
        .input("input0", TableType::TYPE_ID)
        .output("output0", TableType::TYPE_ID)
        .resource(navi::GRAPH_MEMORY_POOL_RESOURCE_ID, true, BIND_RESOURCE_TO(_memoryPoolResource))
        .resource(SqlQueryResource::RESOURCE_ID, true, BIND_RESOURCE_TO(_queryResource))
        .resource(navi::META_INFO_RESOURCE_ID, false, BIND_RESOURCE_TO(_metaInfoResource));
}

bool SortKernel::config(navi::KernelConfigContext &ctx) {
    ctx.Jsonize(IQUAN_OP_ID, _opId, _opId);
    ctx.Jsonize("reuse_inputs", _reuseInputs, _reuseInputs);
    if (!_sortInitParam.initFromJson(ctx)) {
        return false;
    }
    return true;
}

navi::ErrorCode SortKernel::init(navi::KernelInitContext &context) {
    _sortInfo.set_kernelname(getKernelName());
    _sortInfo.set_nodename(getNodeName());
    if (_opId < 0) {
        string keyStr = getKernelName() + "__" + StringUtil::toString(_sortInitParam.keys) + "__"
                        + StringUtil::toString(_sortInitParam.orders);
        _sortInfo.set_hashkey(autil::HashAlgorithm::hashString(keyStr.c_str(), keyStr.size(), 1));
    } else {
        _sortInfo.set_hashkey(_opId);
    }
    _poolPtr = _memoryPoolResource->getPool();
    KERNEL_REQUIRES(_poolPtr, "get pool failed");
    if (_metaInfoResource) {
        _sqlSearchInfoCollector = _metaInfoResource->getOverwriteInfoCollector();
    }
    _queryMetricsReporter = _queryResource->getQueryMetricsReporter();
    return navi::EC_NONE;
}

navi::ErrorCode SortKernel::compute(navi::KernelComputeContext &runContext) {
    incComputeTime();
    uint64_t beginTime = TimeUtility::currentTime();
    navi::PortIndex inputIndex(0, navi::INVALID_INDEX);
    bool eof = false;
    navi::DataPtr data;
    runContext.getInput(inputIndex, data, eof);
    if (data != nullptr) {
        if (!doCompute(data)) {
            SQL_LOG(ERROR, "compute sort limit failed");
            return navi::EC_ABORT;
        }
    }
    incTotalTime(TimeUtility::currentTime() - beginTime);
    if (eof) {
        outputResult(runContext);
        SQL_LOG(DEBUG, "sort info: [%s]", _sortInfo.ShortDebugString().c_str());
    }
    if (_sqlSearchInfoCollector) {
        _sqlSearchInfoCollector->overwriteSortInfo(_sortInfo);
    }
    return navi::EC_NONE;
}

void SortKernel::outputResult(navi::KernelComputeContext &runContext) {
    uint64_t beginTime = TimeUtility::currentTime();
    navi::PortIndex outputIndex(0, navi::INVALID_INDEX);
    if (_comparator != nullptr && _table != nullptr) {
        if (_sortInitParam.offset > 0) {
            size_t offset = std::min(_sortInitParam.offset, _table->getRowCount());
            vector<Row> rows = _table->getRows();
            nth_element(rows.begin(), rows.begin() + offset, rows.end(),
                        [this] (Row a, Row b) { return _comparator->compare(a, b); });
            sort(rows.begin() + offset, rows.end(),
                 [this] (Row a, Row b) { return _comparator->compare(a, b); });
            vector<Row> newRows(rows.begin() + offset, rows.end());
            _table->setRows(newRows);
        } else {
            TableUtil::sort(_table, _comparator.get());
        }
    }
    SQL_LOG(TRACE1, "sort output table: [%s]", TableUtil::toString(_table, 10).c_str());
    TableDataPtr tableData(new TableData(_table));
    runContext.setOutput(outputIndex, tableData, true);
    incOutputTime(TimeUtility::currentTime() - beginTime);
}

bool SortKernel::doCompute(const navi::DataPtr &data) {
    uint64_t beginTime = TimeUtility::currentTime();

    auto inputTable = KernelUtil::getTable(data, _memoryPoolResource, !_reuseInputs.empty());
    if (!inputTable) {
        SQL_LOG(ERROR, "invalid input table");
        return false;
    }
    incTotalInputCount(inputTable->getRowCount());
    if (_comparator == nullptr) {
        _table = inputTable;
        _comparator = ComparatorCreator::createComboComparator(_table, _sortInitParam.keys, _sortInitParam.orders, _poolPtr.get());
        if (_comparator == nullptr) {
            SQL_LOG(ERROR, "init combo comparator failed");
            return false;
        }
    } else {
        if (!_table->merge(inputTable)) {
            SQL_LOG(ERROR, "merge input table failed");
            return false;
        }
        _table->mergeDependentPools(inputTable);
    }
    uint64_t afterMergeTime = TimeUtility::currentTime();
    incMergeTime(afterMergeTime - beginTime);
    TableUtil::topK(_table, _comparator.get(), _sortInitParam.topk);
    uint64_t afterTopKTime = TimeUtility::currentTime();
    incTopKTime(afterTopKTime - afterMergeTime);
    _table->compact();
    incCompactTime(TimeUtility::currentTime() - afterTopKTime);
    SQL_LOG(TRACE1, "sort-topk output table: [%s]", TableUtil::toString(_table, 10).c_str());
    return true;
}

void SortKernel::reportMetrics() {
    if (_queryMetricsReporter != nullptr) {
        string pathName = "sql.user.ops." + getKernelName();
        auto opMetricsReporter = _queryMetricsReporter->getSubReporter(pathName, {});
        opMetricsReporter->report<SortOpMetrics, SortInfo>(nullptr, &_sortInfo);
    }
}

void SortKernel::incComputeTime() {
    _sortInfo.set_totalcomputetimes(_sortInfo.totalcomputetimes() + 1);
}
void SortKernel::incMergeTime(int64_t time) {
    _sortInfo.set_totalmergetime(_sortInfo.totalmergetime() + time);
}

void SortKernel::incTopKTime(int64_t time) {
    _sortInfo.set_totaltopktime(_sortInfo.totaltopktime() + time);
}

void SortKernel::incCompactTime(int64_t time) {
    _sortInfo.set_totalcompacttime(_sortInfo.totalcompacttime() + time);
}

void SortKernel::incOutputTime(int64_t time) {
    _sortInfo.set_totaloutputtime(_sortInfo.totaloutputtime() + time);
}

void SortKernel::incTotalTime(int64_t time) {
    _sortInfo.set_totalusetime(_sortInfo.totalusetime() + time);
}

void SortKernel::incTotalInputCount(size_t count) {
    _sortInfo.set_totalinputcount(_sortInfo.totalinputcount() + count);
}

REGISTER_KERNEL(SortKernel);

} // namespace sql
} // namespace isearch
