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
#include "sql/ops/join/JoinKernelBase.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <engine/KernelInitContext.h>
#include <engine/NaviConfigContext.h>
#include <map>
#include <memory>
#include <stdint.h>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/Pool.h"
#include "kmonitor/client/MetricLevel.h"
#include "kmonitor/client/MetricMacro.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/core/MutableMetric.h"
#include "matchdoc/ValueType.h"
#include "navi/builder/KernelDefBuilder.h"
#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/engine/KernelComputeContext.h"
#include "navi/engine/KernelConfigContext.h"
#include "navi/resource/GraphMemoryPoolR.h"
#include "sql/common/Log.h"
#include "sql/common/common.h"
#include "sql/ops/util/KernelUtil.h"
#include "sql/proto/SqlSearchInfo.pb.h"
#include "table/Table.h"

namespace kmonitor {
class MetricsTags;
} // namespace kmonitor

using namespace std;
using namespace autil;
using namespace matchdoc;
using namespace table;
using namespace kmonitor;

namespace sql {

const int JoinKernelBase::DEFAULT_BATCH_SIZE = 100000;

class JoinOpMetrics : public MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_LATENCY_MUTABLE_METRIC(_totalEvaluateTime, "TotalEvaluateTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_totalJoinTime, "TotalJoinTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_totalHashTime, "TotalHashTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_totalTime, "TotalTime");
        REGISTER_GAUGE_MUTABLE_METRIC(_totalJoinCount, "TotalJoinCount");
        REGISTER_GAUGE_MUTABLE_METRIC(_totalRightHashCount, "TotalRightHashCount");
        REGISTER_GAUGE_MUTABLE_METRIC(_totalLeftHashCount, "TotalLeftHashCount");
        REGISTER_GAUGE_MUTABLE_METRIC(_hashMapSize, "HashMapSize");
        REGISTER_GAUGE_MUTABLE_METRIC(_totalComputeTimes, "TotalComputeTimes");
        REGISTER_GAUGE_MUTABLE_METRIC(_rightScanTime, "RightScanTime");
        REGISTER_GAUGE_MUTABLE_METRIC(_rightUpdateQueryTime, "RightUpdateQueryTime");
        return true;
    }

    void report(const kmonitor::MetricsTags *tags, JoinInfo *joinInfo) {
        REPORT_MUTABLE_METRIC(_totalEvaluateTime, joinInfo->totalevaluatetime() / 1000.0);
        REPORT_MUTABLE_METRIC(_totalJoinTime, joinInfo->totaljointime() / 1000.0);
        REPORT_MUTABLE_METRIC(_totalHashTime, joinInfo->totalhashtime() / 1000.0);
        REPORT_MUTABLE_METRIC(_totalTime, joinInfo->totalusetime() / 1000.0);
        REPORT_MUTABLE_METRIC(_totalJoinCount, joinInfo->totaljoincount());
        REPORT_MUTABLE_METRIC(_totalRightHashCount, joinInfo->totalrighthashcount());
        REPORT_MUTABLE_METRIC(_totalLeftHashCount, joinInfo->totallefthashcount());
        REPORT_MUTABLE_METRIC(_hashMapSize, joinInfo->hashmapsize());
        REPORT_MUTABLE_METRIC(_totalComputeTimes, joinInfo->totalcomputetimes());
        REPORT_MUTABLE_METRIC(_rightScanTime, joinInfo->rightscantime() / 1000.0);
        REPORT_MUTABLE_METRIC(_rightUpdateQueryTime, joinInfo->rightupdatequerytime() / 1000.0);
    }

private:
    MutableMetric *_totalEvaluateTime = nullptr;
    MutableMetric *_totalJoinTime = nullptr;
    MutableMetric *_totalHashTime = nullptr;
    MutableMetric *_totalTime = nullptr;
    MutableMetric *_totalJoinCount = nullptr;
    MutableMetric *_rightScanTime = nullptr;
    MutableMetric *_rightUpdateQueryTime = nullptr;
    MutableMetric *_totalRightHashCount = nullptr;
    MutableMetric *_totalLeftHashCount = nullptr;
    MutableMetric *_hashMapSize = nullptr;
    MutableMetric *_totalComputeTimes = nullptr;
};

JoinKernelBase::JoinKernelBase()
    : _batchSize(DEFAULT_BATCH_SIZE)
    , _truncateThreshold(0)
    , _opId(-1) {}

JoinKernelBase::~JoinKernelBase() {}

bool JoinKernelBase::config(navi::KernelConfigContext &ctx) {
    NAVI_JSONIZE(ctx, "reuse_inputs", _reuseInputs, _reuseInputs);
    NAVI_JSONIZE(ctx, IQUAN_OP_ID, _opId, _opId);
    NAVI_JSONIZE(ctx, "batch_size", _batchSize, _batchSize);
    if (_batchSize == 0) {
        _batchSize = DEFAULT_BATCH_SIZE;
    }
    return doConfig(ctx);
}

navi::ErrorCode JoinKernelBase::init(navi::KernelInitContext &context) {
    patchHintInfo(_joinParamR->_joinHintMap);
    _joinInfoR->_joinInfo->set_kernelname(getKernelName());
    _joinInfoR->_joinInfo->set_nodename(getNodeName());
    if (_opId < 0) {
        const string &keyStr = getKernelName() + "__" + _joinParamR->_conditionJson + "__"
                               + StringUtil::toString(_joinParamR->_outputFieldsForKernel);
        _joinInfoR->_joinInfo->set_hashkey(
            autil::HashAlgorithm::hashString(keyStr.c_str(), keyStr.size(), 1));
    } else {
        _joinInfoR->_joinInfo->set_hashkey(_opId);
    }
    if (!doInit()) {
        return navi::EC_ABORT;
    }
    return navi::EC_NONE;
}

navi::ErrorCode JoinKernelBase::compute(navi::KernelComputeContext &runContext) {
    return navi::EC_ABORT;
}

bool JoinKernelBase::createHashMap(const table::TablePtr &table,
                                   size_t offset,
                                   size_t count,
                                   bool hashLeftTable) {
    autil::ScopedTime2 hashTimer;
    const auto &joinColumns
        = hashLeftTable ? _joinParamR->_leftJoinColumns : _joinParamR->_rightJoinColumns;
    HashJoinMapR::HashValues values;
    if (!_hashJoinMapR->getHashValues(table, offset, count, joinColumns, values)) {
        return false;
    }
    if (hashLeftTable) {
        _joinInfoR->incLeftHashCount(values.size());
    } else {
        _joinInfoR->incRightHashCount(values.size());
    }
    _joinInfoR->incHashTime(hashTimer.done_us());

    autil::ScopedTime2 createTimer;
    _hashJoinMapR->createHashMap(values);
    _joinInfoR->incHashMapSize(_hashJoinMapR->_hashJoinMap.size());
    _joinInfoR->incCreateTime(createTimer.done_us());
    return true;
}

bool JoinKernelBase::getLookupInput(navi::KernelComputeContext &runContext,
                                    const navi::PortIndex &portIndex,
                                    bool leftTable,
                                    table::TablePtr &inputTable,
                                    bool &eof) {
    static const size_t EmptyBufferLimitSize = 1;
    return getInput(runContext, portIndex, leftTable, EmptyBufferLimitSize, inputTable, eof);
}

bool JoinKernelBase::getInput(navi::KernelComputeContext &runContext,
                              const navi::PortIndex &portIndex,
                              bool leftTable,
                              size_t bufferLimitSize,
                              table::TablePtr &inputTable,
                              bool &eof) {
    if (inputTable == nullptr || inputTable->getRowCount() < bufferLimitSize) {
        navi::DataPtr data;
        runContext.getInput(portIndex, data, eof);
        if (data != nullptr) {
            auto table = KernelUtil::getTable(data, _graphMemoryPoolR, !_reuseInputs.empty());
            if (!table) {
                SQL_LOG(ERROR, "invalid input table");
                return false;
            }
            if (leftTable) {
                incTotalLeftInputTable(table->getRowCount());
            } else {
                incTotalRightInputTable(table->getRowCount());
            }
            if (!inputTable) {
                inputTable = table;
            } else {
                if (!inputTable->merge(table)) {
                    SQL_LOG(ERROR, "merge input failed");
                    return false;
                }
            }
        }
    }
    return true;
}

bool JoinKernelBase::canTruncate(size_t joinedCount, size_t truncateThreshold) {
    if (truncateThreshold > 0 && joinedCount >= truncateThreshold) {
        return true;
    }
    return false;
}

void JoinKernelBase::patchHintInfo(const map<string, string> &hintsMap) {
    if (hintsMap.empty()) {
        return;
    }
    auto iter = hintsMap.find("batchSize");
    if (iter != hintsMap.end()) {
        uint32_t batchSize = 0;
        StringUtil::fromString(iter->second, batchSize);
        if (batchSize > 0) {
            _batchSize = batchSize;
        }
    }
    iter = hintsMap.find("truncateThreshold");
    if (iter != hintsMap.end()) {
        uint32_t truncateThreshold = 0;
        StringUtil::fromString(iter->second, truncateThreshold);
        if (truncateThreshold > 0) {
            _truncateThreshold = truncateThreshold;
            SQL_LOG(DEBUG, "truncate threshold [%ld]", _truncateThreshold);
        }
    }
}

void JoinKernelBase::reportMetrics() {
    if (_queryMetricReporterR) {
        string kernelName = getKernelName();
        KernelUtil::stripKernelNamePrefix(kernelName);
        string pathName = "sql.user.ops." + std::move(kernelName);
        auto opMetricsReporter = _queryMetricReporterR->getReporter()->getSubReporter(pathName, {});
        opMetricsReporter->report<JoinOpMetrics, JoinInfo>(nullptr, _joinInfoR->_joinInfo.get());
    }
}

void JoinKernelBase::incTotalLeftInputTable(size_t count) {
    _joinInfoR->incTotalLeftInputTable(count);
}

void JoinKernelBase::incTotalRightInputTable(size_t count) {
    _joinInfoR->incTotalLeftInputTable(count);
}

} // namespace sql
