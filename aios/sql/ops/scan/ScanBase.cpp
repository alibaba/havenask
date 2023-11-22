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
#include "sql/ops/scan/ScanBase.h"

#include <engine/ResourceInitContext.h>
#include <limits>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/DataBuffer.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/Pool.h"
#include "ha3/common/Term.h"
#include "ha3/search/PartitionInfoWrapper.h"
#include "indexlib/partition/partition_group_resource.h"
#include "kmonitor/client/MetricLevel.h"
#include "kmonitor/client/MetricMacro.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/core/MutableMetric.h"
#include "matchdoc/CommonDefine.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/ValueType.h"
#include "navi/log/NaviLogger.h"
#include "navi/resource/GraphMemoryPoolR.h"
#include "sql/common/Log.h"
#include "sql/framework/PushDownOp.h"
#include "sql/ops/util/KernelUtil.h"
#include "sql/proto/SqlSearchInfo.pb.h"
#include "sql/proto/SqlSearchInfoCollector.h"
#include "suez/turing/common/JoinConfigInfo.h"
#include "table/Table.h"
#include "table/TableUtil.h"

namespace kmonitor {
class MetricsTags;
} // namespace kmonitor

using namespace std;
using namespace autil;
using namespace table;
using namespace matchdoc;
using namespace suez::turing;
using namespace kmonitor;

using namespace isearch::search;
using namespace isearch::common;

namespace sql {

class InnerScanOpMetrics : public MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_GAUGE_MUTABLE_METRIC(_totalSeekDocCount, "TotalSeekDocCount");
        REGISTER_GAUGE_MUTABLE_METRIC(_useTruncate, "UseTruncate");
        return true;
    }

    void report(const kmonitor::MetricsTags *tags, InnerScanInfo *innerScanInfo) {
        REPORT_MUTABLE_METRIC(_totalSeekDocCount, innerScanInfo->totalseekdoccount());
        REPORT_MUTABLE_METRIC(_useTruncate, innerScanInfo->usetruncate());
    }

private:
    MutableMetric *_totalSeekDocCount = nullptr;
    MutableMetric *_useTruncate = nullptr;
};

class ScanOpMetrics : public MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_GAUGE_MUTABLE_METRIC(_totalComputeCount, "TotalComputeCount");
        REGISTER_GAUGE_MUTABLE_METRIC(_totalOutputCount, "TotalOutputCount");
        REGISTER_GAUGE_MUTABLE_METRIC(_totalScanCount, "TotalScanCount");
        REGISTER_GAUGE_MUTABLE_METRIC(_queryPoolSize, "queryPoolSize");
        REGISTER_LATENCY_MUTABLE_METRIC(_totalSeekTime, "TotalSeekTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_totalEvaluateTime, "TotalEvaluateTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_totalOutputTime, "TotalOutputTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_totalInitTime, "TotalInitTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_totalComputeTime, "TotalComputeTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_durationTime, "DurationTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_watermarkToCurrent, "WatermarkToCurrent");
        return true;
    }

    void report(const kmonitor::MetricsTags *tags, ScanInfo *scanInfo) {
        REPORT_MUTABLE_METRIC(_totalComputeCount, scanInfo->totalcomputetimes());
        REPORT_MUTABLE_METRIC(_totalOutputCount, scanInfo->totaloutputcount());
        REPORT_MUTABLE_METRIC(_totalScanCount, scanInfo->totalscancount());
        REPORT_MUTABLE_METRIC(_queryPoolSize, scanInfo->querypoolsize() / 1000);
        REPORT_MUTABLE_METRIC(_totalSeekTime, scanInfo->totalseektime() / 1000);
        REPORT_MUTABLE_METRIC(_totalEvaluateTime, scanInfo->totalevaluatetime() / 1000);
        REPORT_MUTABLE_METRIC(_totalOutputTime, scanInfo->totaloutputtime() / 1000);
        REPORT_MUTABLE_METRIC(_totalInitTime, scanInfo->totalinittime() / 1000);
        REPORT_MUTABLE_METRIC(_totalComputeTime, scanInfo->totalusetime() / 1000);
        REPORT_MUTABLE_METRIC(_durationTime, scanInfo->durationtime() / 1000);
        if (scanInfo->buildwatermark() > 0) {
            REPORT_MUTABLE_METRIC(_watermarkToCurrent,
                                  (TimeUtility::currentTime() - scanInfo->buildwatermark()) / 1000);
        }
    }

private:
    MutableMetric *_totalComputeCount = nullptr;
    MutableMetric *_totalOutputCount = nullptr;
    MutableMetric *_totalScanCount = nullptr;
    MutableMetric *_queryPoolSize = nullptr;
    MutableMetric *_totalSeekTime = nullptr;
    MutableMetric *_totalEvaluateTime = nullptr;
    MutableMetric *_totalOutputTime = nullptr;
    MutableMetric *_totalInitTime = nullptr;
    MutableMetric *_totalComputeTime = nullptr;
    MutableMetric *_durationTime = nullptr;
    MutableMetric *_watermarkToCurrent = nullptr;
};

ScanBase::ScanBase()
    : _durationTimer(make_unique<autil::ScopedTime2>())
    , _scanOnce(true)
    , _pushDownMode(false)
    , _enableWatermark(false)
    , _batchSize(0)
    , _limit(std::numeric_limits<uint32_t>::max())
    , _scanCount(0) {}

ScanBase::~ScanBase() {
    reportBaseMetrics();
}

bool ScanBase::doInit() {
    _queryConfig = &_queryConfigData->getConfig();
    _batchSize = _scanInitParamR->batchSize;
    _limit = _scanInitParamR->limit;
    return true;
}

bool ScanBase::batchScan(table::TablePtr &table, bool &eof) {
    _scanInitParamR->incComputeTime();

    autil::ScopedTime2 batchScanTimer;
    auto ret = doBatchScan(table, eof);
    _scanInitParamR->updateDurationTime(_durationTimer ? _durationTimer->done_us() : 0);
    _scanInitParamR->incTotalTime(batchScanTimer.done_us());

    _scanInitParamR->scanInfo.set_querypoolsize(_queryMemPoolR->getPool()->getAllocatedSize());
    if (!ret) {
        onBatchScanFinish();
        return false;
    }
    if (_scanInitParamR->scanInfo.totalcomputetimes() < 5 && table != nullptr) {
        SQL_LOG(TRACE2,
                "scan id [%d] output table [%s]: [%s]",
                _scanInitParamR->parallelIndex,
                _scanInitParamR->tableName.c_str(),
                TableUtil::toString(table, 10).c_str());
    }

    if (table) {
        _scanInitParamR->incTotalOutputCount(table->getRowCount());
    }
    _scanInitParamR->setTotalScanCount(_scanCount);
    if (_sqlSearchInfoCollectorR) {
        _sqlSearchInfoCollectorR->getCollector()->overwriteScanInfo(_scanInitParamR->scanInfo);
    }
    if (eof) {
        SQL_LOG(TRACE1, "scan info: [%s]", _scanInitParamR->scanInfo.ShortDebugString().c_str());
        onBatchScanFinish();
    }
    if (_scanPushDownR) {
        if (!_scanPushDownR->compute(table, eof)) {
            return false;
        }
    }
    return true;
}

bool ScanBase::updateScanQuery(const StreamQueryPtr &inputQuery) {
    autil::ScopedTime2 updateScanQueryTimer;
    auto ret = doUpdateScanQuery(inputQuery);
    _scanInitParamR->incUpdateScanQueryTime(updateScanQueryTimer.done_us());
    return ret;
}

table::TablePtr ScanBase::createTable(std::vector<matchdoc::MatchDoc> &matchDocVec,
                                      const matchdoc::MatchDocAllocatorPtr &matchDocAllocator,
                                      bool reuseMatchDocAllocator) {
    vector<MatchDoc> copyMatchDocs;
    MatchDocAllocatorPtr outputAllocator = copyMatchDocAllocator(
        matchDocVec, matchDocAllocator, reuseMatchDocAllocator, copyMatchDocs);

    return doCreateTable(std::move(outputAllocator), std::move(copyMatchDocs));
}

MatchDocAllocatorPtr ScanBase::copyMatchDocAllocator(vector<MatchDoc> &matchDocVec,
                                                     const MatchDocAllocatorPtr &matchDocAllocator,
                                                     bool reuseMatchDocAllocator,
                                                     vector<MatchDoc> &copyMatchDocs) {
    MatchDocAllocatorPtr outputAllocator = matchDocAllocator;
    if (!reuseMatchDocAllocator) {
        if (_tableMeta.empty()) {
            auto tempBufPool = _graphMemoryPoolR->getPool();
            DataBuffer dataBuffer(DataBuffer::DEFAUTL_DATA_BUFFER_SIZE, tempBufPool.get());
            matchDocAllocator->setSortRefFlag(false);
            matchDocAllocator->serializeMeta(dataBuffer, SL_ATTRIBUTE);
            _tableMeta.append(dataBuffer.getData(), dataBuffer.getDataLen());
        }
        DataBuffer dataBuffer((void *)_tableMeta.c_str(), _tableMeta.size());
        outputAllocator.reset(new MatchDocAllocator(_graphMemoryPoolR->getPool()));
        outputAllocator->deserializeMeta(dataBuffer, nullptr);
        bool fastCopySucc
            = matchDocAllocator->swapDocStorage(*outputAllocator, copyMatchDocs, matchDocVec);

        if (!fastCopySucc) {
            auto tempBufPool = _graphMemoryPoolR->getPool();
            DataBuffer writeBuffer(DataBuffer::DEFAUTL_DATA_BUFFER_SIZE, tempBufPool.get());
            matchDocAllocator->serialize(writeBuffer, matchDocVec, SL_ATTRIBUTE);

            auto dataPool = _graphMemoryPoolR->getPool();
            DataBuffer readBuffer(writeBuffer.getData(), writeBuffer.getDataLen(), dataPool.get());
            outputAllocator.reset(new MatchDocAllocator(dataPool));
            outputAllocator->deserialize(readBuffer, copyMatchDocs);
            matchDocAllocator->deallocate(matchDocVec.data(), matchDocVec.size());
        }
    } else {
        copyMatchDocs.swap(matchDocVec);
    }
    return outputAllocator;
}

table::TablePtr ScanBase::doCreateTable(matchdoc::MatchDocAllocatorPtr outputAllocator,
                                        std::vector<matchdoc::MatchDoc> copyMatchDocs) {
    return Table::fromMatchDocs(copyMatchDocs, outputAllocator);
}

bool ScanBase::initAsyncPipe(navi::ResourceInitContext &ctx) {
    auto asyncPipe = ctx.createRequireKernelAsyncPipe();
    if (!asyncPipe) {
        NAVI_KERNEL_LOG(ERROR, "create async pipe failed");
        return false;
    }
    setAsyncPipe(asyncPipe);
    return true;
}

void ScanBase::reportBaseMetrics() {
    if (_queryMetricReporterR) {
        static const string pathName = "sql.user.ops.ScanKernel";
        auto opMetricsReporter = _queryMetricReporterR->getReporter()->getSubReporter(
            pathName,
            {{{"table_name", getTableNameForMetrics()}, {"op_scope", _scanInitParamR->opScope}}});
        opMetricsReporter->report<ScanOpMetrics, ScanInfo>(nullptr, &_scanInitParamR->scanInfo);
        opMetricsReporter->report<InnerScanOpMetrics, InnerScanInfo>(nullptr, &_innerScanInfo);
    }
}

} // namespace sql
