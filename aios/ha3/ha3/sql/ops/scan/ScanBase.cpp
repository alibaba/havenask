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
#include "ha3/sql/ops/scan/ScanBase.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/EnvUtil.h"
#include "autil/HashAlgorithm.h"
#include "autil/TimeUtility.h"
#include "autil/TimeoutTerminator.h"
#include "autil/legacy/exception.h"
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/search/IndexPartitionReaderUtil.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/sql/common/IndexInfo.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/common/TableDistribution.h"
#include "ha3/sql/common/common.h"
#include "ha3/sql/ops/scan/Collector.h"
#include "ha3/sql/ops/scan/ScanResource.h"
#include "ha3/sql/ops/util/KernelUtil.h"
#include "ha3/sql/proto/SqlSearchInfo.pb.h"
#include "ha3/sql/proto/SqlSearchInfoCollector.h"
#include "ha3/sql/resource/ObjectPoolResource.h"
#include "indexlib/misc/common.h"
#include "indexlib/config/legacy_schema_adapter.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/partition/partition_reader_snapshot.h"
#include "kmonitor/client/MetricMacro.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/core/MutableMetric.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/VectorDocStorage.h"
#include "navi/engine/KernelConfigContext.h"
#include "navi/resource/GraphMemoryPoolResource.h"
#include "rapidjson/rapidjson.h"
#include "suez/turing/expression/framework/AttributeExpressionCreator.h"
#include "suez/turing/expression/provider/FunctionProvider.h"
#include "suez/turing/expression/util/TableInfo.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/TableUtil.h"

namespace indexlib {
namespace partition {
class PartitionReaderSnapshot;
} // namespace partition
} // namespace indexlib

namespace kmonitor {
class MetricsTags;
} // namespace kmonitor
namespace suez {
namespace turing {
class CavaPluginManager;
class FunctionInterfaceCreator;
class SuezCavaAllocator;
class Tracer;
class VirtualAttribute;
} // namespace turing
} // namespace suez

using namespace std;
using namespace autil;
using namespace table;
using namespace matchdoc;
using namespace suez::turing;
using namespace kmonitor;

using namespace isearch::search;
using namespace isearch::common;

namespace isearch {
namespace sql {

AUTIL_LOG_SETUP(sql, ScanBase);
AUTIL_LOG_SETUP(sql, ScanInitParam);

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
        REGISTER_GAUGE_MUTABLE_METRIC(_totalSeekCount, "TotalSeekCount");
        REGISTER_GAUGE_MUTABLE_METRIC(_queryPoolSize, "queryPoolSize");
        REGISTER_LATENCY_MUTABLE_METRIC(_totalSeekTime, "TotalSeekTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_totalEvaluateTime, "TotalEvaluateTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_totalOutputTime, "TotalOutputTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_totalInitTime, "TotalInitTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_totalComputeTime, "TotalComputeTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_durationTime, "DurationTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_isLeader, "isLeader");
        REGISTER_LATENCY_MUTABLE_METRIC(_watermarkToCurrent, "WatermarkToCurrent");
        return true;
    }

    void report(const kmonitor::MetricsTags *tags, ScanInfo *scanInfo) {
        REPORT_MUTABLE_METRIC(_totalComputeCount, scanInfo->totalcomputetimes());
        REPORT_MUTABLE_METRIC(_totalOutputCount, scanInfo->totaloutputcount());
        REPORT_MUTABLE_METRIC(_totalScanCount, scanInfo->totalscancount());
        REPORT_MUTABLE_METRIC(_totalSeekCount, scanInfo->totalseekcount());
        REPORT_MUTABLE_METRIC(_queryPoolSize, scanInfo->querypoolsize() / 1000);
        REPORT_MUTABLE_METRIC(_totalSeekTime, scanInfo->totalseektime() / 1000);
        REPORT_MUTABLE_METRIC(_totalEvaluateTime, scanInfo->totalevaluatetime() / 1000);
        REPORT_MUTABLE_METRIC(_totalOutputTime, scanInfo->totaloutputtime() / 1000);
        REPORT_MUTABLE_METRIC(_totalInitTime, scanInfo->totalinittime() / 1000);
        REPORT_MUTABLE_METRIC(_totalComputeTime, scanInfo->totalusetime() / 1000);
        REPORT_MUTABLE_METRIC(_durationTime, scanInfo->durationtime() / 1000);
        REPORT_MUTABLE_METRIC(_isLeader, scanInfo->isleader());
        if (scanInfo->buildwatermark() > 0 ) {
            REPORT_MUTABLE_METRIC(_watermarkToCurrent, (TimeUtility::currentTime() - scanInfo->buildwatermark()) / 1000);
        }
    }

private:
    MutableMetric *_totalComputeCount = nullptr;
    MutableMetric *_totalOutputCount = nullptr;
    MutableMetric *_totalScanCount = nullptr;
    MutableMetric *_totalSeekCount = nullptr;
    MutableMetric *_queryPoolSize = nullptr;
    MutableMetric *_totalSeekTime = nullptr;
    MutableMetric *_totalEvaluateTime = nullptr;
    MutableMetric *_totalOutputTime = nullptr;
    MutableMetric *_totalInitTime = nullptr;
    MutableMetric *_totalComputeTime = nullptr;
    MutableMetric *_durationTime = nullptr;
    MutableMetric *_isLeader = nullptr;
    MutableMetric *_watermarkToCurrent = nullptr;
};

ScanInitParam::ScanInitParam()
    : scanResource()
    , targetWatermark(0)
    , targetWatermarkType(WatermarkType::WM_TYPE_DISABLED)
    , batchSize(0)
    , limit(std::numeric_limits<uint32_t>::max())
    , parallelNum(1)
    , parallelIndex(0)
    , opId(-1)
    , useNest(false)
    , reserveMaxCount(0)
    , opScope("default")
{}

bool ScanInitParam::initFromJson(navi::KernelConfigContext &ctx) {
    try {
        ctx.Jsonize("parallel_num", parallelNum, parallelNum);
        ctx.Jsonize("parallel_index", parallelIndex, parallelIndex);
        if (0 == parallelNum || parallelIndex >= parallelNum) {
            SQL_LOG(ERROR,
                    "illegal parallel param:parallelNum[%u],parallelIndex[%u]",
                    parallelNum,
                    parallelIndex);
            return false;
        }
        ctx.Jsonize("table_name", tableName);
        ctx.Jsonize("aux_table_name", auxTableName, auxTableName);
        ctx.Jsonize("output_fields_internal", outputFields, outputFields);
        ctx.Jsonize("table_type", tableType);
        ctx.Jsonize("db_name", dbName, dbName);
        ctx.Jsonize("catalog_name", catalogName, catalogName);
        ctx.Jsonize("output_fields_internal_type", outputFieldsType, outputFieldsType);
        ctx.Jsonize("table_meta", tableMeta, tableMeta);
        ctx.Jsonize(SCAN_TARGET_WATERMARK, targetWatermark, targetWatermark);
        ctx.Jsonize(SCAN_TARGET_WATERMARK_TYPE, targetWatermarkType, targetWatermarkType);
        ctx.Jsonize("batch_size", batchSize, batchSize);
        ctx.Jsonize("use_nest_table", useNest, useNest);
        ctx.Jsonize("nest_table_attrs", nestTableAttrs, nestTableAttrs);
        ctx.Jsonize("limit", limit, limit);
        ctx.Jsonize("hints", hintsMap, hintsMap);
        ctx.Jsonize("hash_type", hashType, hashType);
        ctx.Jsonize("hash_fields", hashFields, hashFields);
        ctx.Jsonize("table_distribution", tableDist, tableDist);
        if (isRemoteScan()) {
            ctx.Jsonize("location", location, location);
        }
        ctx.Jsonize(IQUAN_OP_ID, opId, opId);
        tableMeta.extractIndexInfos(indexInfos);
        tableMeta.extractFieldInfos(fieldInfos);
        SQL_LOG(TRACE3, "fetch index infos [%s]", StringUtil::toString(indexInfos).c_str());
        SQL_LOG(TRACE3, "fetch field infos [%s]", StringUtil::toString(fieldInfos).c_str());
        if (tableDist.hashMode._hashFields.size() > 0) { // compatible with old plan
            hashType = tableDist.hashMode._hashFunction;
            hashFields = tableDist.hashMode._hashFields;
        }
        // TODO
        if (tableDist.partCnt > 1 && hashFields.empty()) {
            SQL_LOG(ERROR, "hash fields is empty.");
            return false;
        }
        ctx.Jsonize("used_fields", usedFields, usedFields);

        if (ctx.hasKey("aggregation_index_name")) {
            ctx.Jsonize("aggregation_index_name", aggIndexName, aggIndexName);
            ctx.Jsonize("aggregation_keys", aggKeys, aggKeys);
            ctx.Jsonize("aggregation_type", aggType, aggType);
            ctx.Jsonize("aggregation_value_field", aggValueField, aggValueField);
            ctx.Jsonize("aggregation_distinct", aggDistinct, aggDistinct);
            ctx.Jsonize("aggregation_range_map", aggRangeMap, aggRangeMap);
        }

        if (ctx.hasKey("push_down_ops")) {
            auto pushDownOpsCtx = ctx.enter("push_down_ops");
            if (!pushDownOpsCtx.isArray()) {
                SQL_LOG(ERROR, "push down ops not array.");
                return false;
            }
            if (pushDownOpsCtx.size() > 0) {
                auto firstOpCtx = pushDownOpsCtx.enter(0);
                string opName;
                firstOpCtx.Jsonize("op_name", opName);
                if (opName == "CalcOp") {
                    auto attrCtx = firstOpCtx.enter("attrs");
                    attrCtx.JsonizeAsString("condition", conditionJson, "");
                    attrCtx.JsonizeAsString("output_field_exprs", outputExprsJson, "");
                }
            }
            if (pushDownOpsCtx.size() > 1) {
                auto lastOpCtx = pushDownOpsCtx.enter(1);
                lastOpCtx.Jsonize("op_name", opName);
                if (opName == "SortOp") {
                    auto attrCtx = lastOpCtx.enter("attrs");
                    SQL_LOG(TRACE2, "parse sort desc [%s]", StringUtil::toString(attrCtx).c_str());
                    if (!sortDesc.initFromJson(attrCtx)) {
                        SQL_LOG(ERROR, "parse sort desc [%s] failed", StringUtil::toString(attrCtx).c_str());
                        return false;
                    }
                }
            }
        } else {
            // compatible ut
            ctx.JsonizeAsString("condition", conditionJson, "");
            ctx.JsonizeAsString("output_field_exprs", outputExprsJson, "");
        }

        if (outputExprsJson.empty()) {
            outputExprsJson = "{}";
        }
        ctx.Jsonize("match_type", matchType, matchType);
        ctx.Jsonize("op_scope", opScope, opScope);
    } catch (const autil::legacy::ExceptionBase &e) {
        SQL_LOG(ERROR, "scanInitParam init failed error:[%s].", e.what());
        return false;
    } catch (...) { return false; }
    rawOutputFields = outputFields;
    return true;
}

void ScanInitParam::forbidIndex(const map<string, map<string, string>> &hintsMap,
                                map<string, IndexInfo> &indexInfos,
                                unordered_set<string> &forbidIndexs)
{
    const auto &mapIter = hintsMap.find(SQL_SCAN_HINT);
    if (mapIter == hintsMap.end()) {
        return;
    }
    const map<string, string> &hints = mapIter->second;
    if (hints.empty()) {
        return;
    }
    const auto &iter = hints.find("forbidIndex");
    if (iter != hints.end()) {
        vector<string> indexList = autil::StringUtil::split(iter->second, ",");
        for (const auto &index : indexList) {
            indexInfos.erase(index);
            forbidIndexs.insert(index);
        }
        SQL_LOG(TRACE3, "after forbid index, index infos [%s]",
            StringUtil::toString(indexInfos).c_str());
    }
}

bool ScanInitParam::isRemoteScan() {
    const auto &mapIter = hintsMap.find(SQL_SCAN_HINT);
    if (mapIter == hintsMap.end()) {
        return false;
    }
    const auto &scanHints = mapIter->second;
    return scanHints.find("remoteSourceType") != scanHints.end();
}

ScanBase::ScanBase(const ScanInitParam &param)
    : _enableTableInfo(true)
    , _useSub(false)
    , _scanOnce(true)
    , _pushDownMode(false)
    , _batchSize(0)
    , _limit(std::numeric_limits<uint32_t>::max())
    , _seekCount(0)
    , _param(param)
    , _matchDataManager(new MatchDataManager())
{
}

ScanBase::~ScanBase() {
    reportBaseMetrics();
    _matchDocAllocator.reset();
    _indexPartitionReaderWrapper.reset();
    _attributeExpressionCreator.reset();
    _functionProvider.reset();
}

bool ScanBase::init() {
    autil::ScopedTime2 initTimer;
    _durationTimer = make_unique<autil::ScopedTime2>();
    if (!preInit()) {
        SQL_LOG(ERROR, "pre init failed");
        return false;
    }
    bool ret = doInit();
    incInitTime(initTimer.done_us());
    return ret;
}

bool ScanBase::preInit() {
    _batchSize = _param.batchSize;
    _limit = _param.limit;
    _isLeader = _param.scanResource.isLeader(_param.tableName);
    _scanInfo.set_tablename(_param.tableName);
    _scanInfo.set_kernelname(_param.opName);
    _scanInfo.set_nodename(_param.nodeName);
    _scanInfo.set_parallelid(_param.parallelIndex);
    _scanInfo.set_parallelnum(_param.parallelNum);
    _scanInfo.set_isleader(_isLeader);
    patchHintInfo(_param.hintsMap);
    SQL_LOG(DEBUG,
            "scan table [%s] isLeader [%d] in ip [%s], part id [%s], limit[%u].",
            _param.tableName.c_str(),
            _isLeader,
            autil::EnvUtil::getEnv("ip", "").c_str(),
            autil::EnvUtil::getEnv("partId", "").c_str(),
            _limit);

    if (_param.opId < 0) {
        const string &keyStr
            = _param.opName + "__" + _param.tableName + "__" + _param.conditionJson;
        _scanInfo.set_hashkey(autil::HashAlgorithm::hashString(keyStr.c_str(), keyStr.size(), 1));
    } else {
        _scanInfo.set_hashkey(_param.opId);
    }
    auto &scanResource = _param.scanResource;
    if (!_param.scanResource.memoryPoolResource) {
        SQL_LOG(ERROR, "get mem pool resource failed.");
        return false;
    }
    KERNEL_REQUIRES(_param.scanResource.queryPool, "get pool failed");
    _modelConfigMap = scanResource.modelConfigMap;
    _udfToQueryManager = scanResource.udfToQueryManager;

    int64_t timeoutMs = scanResource.timeoutMs;
    if (timeoutMs > 0) {
        int64_t leftTimeout = timeoutMs * 1000;
        _timeoutTerminator.reset(
            new TimeoutTerminator(leftTimeout * SCAN_TIMEOUT_PERCENT, scanResource.queryStartTime));
        _timeoutTerminator->init(1 << 12);
    }

    if (_enableTableInfo) {
        const string &tableName = _param.tableName;
        _tableInfo = scanResource.getTableInfo(tableName);
        if (!_tableInfo) {
            SQL_LOG(ERROR, "table [%s] not exist.", tableName.c_str());
            return false;
        }

        _indexPartitionReaderWrapper
            = IndexPartitionReaderUtil::createIndexPartitionReaderWrapper(
                    scanResource.partitionReaderSnapshot, tableName);
        if (_param.useNest) {
            const string &subTableName = _tableInfo->getSubTableName();
            for (auto &nestTableAttr : _param.nestTableAttrs) {
                if (subTableName == nestTableAttr.tableName) {
                    _useSub = true;
                    SQL_LOG(TRACE1, "table [%s] use sub doc.", tableName.c_str());
                } else {
                    SQL_LOG(ERROR,
                            "table [%s] need unnest multi value, not support.[%s != %s]",
                            tableName.c_str(),
                            subTableName.c_str(),
                            nestTableAttr.tableName.c_str());
                    return false;
                }
            }
        }
        _indexPartitionReaderWrapper->setSessionPool(_param.scanResource.queryPool);
        if (!_param.auxTableName.empty() && scanResource.tableInfoWithRel == nullptr) {
            SQL_LOG(ERROR, "relation table info not exist.");
            return false;
        }

        if (!prepareMatchDocAllocator()) {
            SQL_LOG(ERROR, "table [%s] prepare matchdoc allocator failed", tableName.c_str());
            return false;
        }

        if (SCAN_KV_TABLE_TYPE != _param.tableType && SCAN_KKV_TABLE_TYPE != _param.tableType &&
            SCAN_AGGREGATION_TABLE_TYPE != _param.tableType &&
            !initExpressionCreator(_param.auxTableName.empty() ? _tableInfo : scanResource.tableInfoWithRel,
                                   _useSub,
                                   scanResource.functionInterfaceCreator,
                                   scanResource.cavaPluginManager,
                                   scanResource.suezCavaAllocator,
                                   scanResource.tracer,
                                   scanResource.partitionReaderSnapshot)) {
            SQL_LOG(WARN, "table [%s] init expression creator failed.", tableName.c_str());
            return false;
        }
    }
    return true;
}

bool ScanBase::prepareMatchDocAllocator() {
    auto poolPtr = _param.scanResource.memoryPoolResource->getPool();
    if (poolPtr == nullptr) {
        return false;
    }
    _matchDocAllocator = CollectorUtil::createMountedMatchDocAllocator(_indexPartitionReaderWrapper->getSchema(), poolPtr);
    return true;
}


bool ScanBase::initExpressionCreator(
    const TableInfoPtr &tableInfo,
    bool useSub,
    suez::turing::FunctionInterfaceCreator *functionInterfaceCreator,
    suez::turing::CavaPluginManager *cavaPluginManager,
    suez::turing::SuezCavaAllocator *suezCavaAllocator,
    suez::turing::Tracer *tracer,
    indexlib::partition::PartitionReaderSnapshot *partitionReaderSnapshot) {

    bool useSubFlag = useSub && tableInfo->hasSubSchema();

    _matchDocAllocator.reset(
            new MatchDocAllocator(_param.scanResource.memoryPoolResource->getPool(), useSubFlag));
    _functionProvider.reset(new FunctionProvider(_matchDocAllocator.get(),
                    _param.scanResource.queryPool,
                    suezCavaAllocator,
                    tracer,
                    partitionReaderSnapshot,
                    NULL));
    std::vector<suez::turing::VirtualAttribute *> virtualAttributes;
    _attributeExpressionCreator.reset(new AttributeExpressionCreator(_param.scanResource.queryPool,
                                                                     _matchDocAllocator.get(),
                                                                     _param.tableName,
                                                                     partitionReaderSnapshot,
                                                                     tableInfo,
                                                                     virtualAttributes,
                                                                     functionInterfaceCreator,
                                                                     cavaPluginManager,
                                                                     _functionProvider.get(),
                                                                     tableInfo->getSubTableName()));
    return true;
}

bool ScanBase::batchScan(TablePtr &table, bool &eof) {
    incComputeTime();

    autil::ScopedTime2 batchScanTimer;
    auto ret = doBatchScan(table, eof);
    updateDurationTime(_durationTimer ? _durationTimer->done_us() : 0);
    incTotalTime(batchScanTimer.done_us());

    _scanInfo.set_querypoolsize(_param.scanResource.queryPool->getAllocatedSize());
    if (!ret) {
        onBatchScanFinish();
        return false;
    }
    if (_scanInfo.totalcomputetimes() < 5 && table != nullptr) {
        SQL_LOG(TRACE1,
                "scan id [%d] output table [%s]: [%s]",
                _param.parallelIndex,
                _param.tableName.c_str(),
                TableUtil::toString(table, 10).c_str());
    }

    if (table) {
        incTotalOutputCount(table->getRowCount());
    }
    setTotalSeekCount(_seekCount);
    if (_param.scanResource.searchInfoCollector) {
        _param.scanResource.searchInfoCollector->overwriteScanInfo(_scanInfo);
    }
    if (eof) {
        SQL_LOG(DEBUG, "scan info: [%s]", _scanInfo.ShortDebugString().c_str());
        onBatchScanFinish();
    }
    return true;
}

bool ScanBase::updateScanQuery(const StreamQueryPtr &inputQuery) {
    autil::ScopedTime2 updateScanQueryTimer;
    auto ret = doUpdateScanQuery(inputQuery);
    incUpdateScanQueryTime(updateScanQueryTimer.done_us());
    return ret;
}

void ScanBase::setMatchDataCollectorBaseConfig(
    search::MatchDataCollectorBase *matchDataCollectorBase) {
    matchDataCollectorBase->setMatchDocAllocator(_matchDocAllocator.get());
    matchDataCollectorBase->setPool(_param.scanResource.queryPool);
}

void ScanBase::patchHintInfo(const map<string, map<string, string>> &hintsMap) {
    const auto &mapIter = hintsMap.find(SQL_SCAN_HINT);
    if (mapIter == hintsMap.end()) {
        return;
    }
    const map<string, string> &hints = mapIter->second;
    if (hints.empty()) {
        return;
    }

    uint32_t localLimit = 0;
    if (fromHint(hints, "localLimit", localLimit) && localLimit > 0) {
        if (_limit > 0) {
            _limit = min(_limit, localLimit);
        } else {
            _limit = localLimit;
        }
    }

    uint32_t batchSize = 0;
    if (fromHint(hints, "batchSize", batchSize) && batchSize > 0) {
        _batchSize = batchSize;
    }

    patchHintInfo(hints);
}

TablePtr ScanBase::createTable(vector<MatchDoc> &matchDocVec,
                               const MatchDocAllocatorPtr &matchDocAllocator,
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
            auto tempBufPool = _param.scanResource.memoryPoolResource->getPool();
            DataBuffer dataBuffer(DataBuffer::DEFAUTL_DATA_BUFFER_SIZE, tempBufPool.get());
            matchDocAllocator->setSortRefFlag(false);
            matchDocAllocator->serializeMeta(dataBuffer, SL_ATTRIBUTE);
            _tableMeta.append(dataBuffer.getData(), dataBuffer.getDataLen());
        }
        DataBuffer dataBuffer((void *)_tableMeta.c_str(), _tableMeta.size());
        outputAllocator.reset(new MatchDocAllocator(_param.scanResource.memoryPoolResource->getPool()));
        outputAllocator->deserializeMeta(dataBuffer, nullptr);
        bool fastCopySucc
            = matchDocAllocator->swapDocStorage(*outputAllocator, copyMatchDocs, matchDocVec);

        if (!fastCopySucc) {
            auto tempBufPool = _param.scanResource.memoryPoolResource->getPool();
            DataBuffer writeBuffer(DataBuffer::DEFAUTL_DATA_BUFFER_SIZE, tempBufPool.get());
            matchDocAllocator->serialize(writeBuffer, matchDocVec, SL_ATTRIBUTE);

            auto dataPool = _param.scanResource.memoryPoolResource->getPool();
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

TablePtr ScanBase::doCreateTable(MatchDocAllocatorPtr outputAllocator,
                                 vector<MatchDoc> copyMatchDocs) {
    return make_shared<Table>(copyMatchDocs, outputAllocator);
}

void ScanBase::reportBaseMetrics() {
    if (_param.scanResource.metricsReporter != nullptr) {
        const string &pathName = "sql.user.ops." + _param.opName;
        auto opMetricsReporter = _param.scanResource.metricsReporter->getSubReporter(
                pathName, {{{"table_name", getTableNameForMetrics()},
                            {"op_scope", _param.opScope}}});
        opMetricsReporter->report<ScanOpMetrics, ScanInfo>(nullptr, &_scanInfo);
        opMetricsReporter->report<InnerScanOpMetrics, InnerScanInfo>(nullptr, &_innerScanInfo);
    }
}

void ScanBase::incComputeTime() {
    _scanInfo.set_totalcomputetimes(_scanInfo.totalcomputetimes() + 1);
}

void ScanBase::incInitTime(int64_t time) {
    _scanInfo.set_totalinittime(_scanInfo.totalinittime() + time);
    incTotalTime(time);
}

void ScanBase::incUpdateScanQueryTime(int64_t time) {
    // TODO: use own timer
    incInitTime(time);
}

void ScanBase::incSeekTime(int64_t time) {
    _scanInfo.set_totalseektime(_scanInfo.totalseektime() + time);
}

void ScanBase::incEvaluateTime(int64_t time) {
    _scanInfo.set_totalevaluatetime(_scanInfo.totalevaluatetime() + time);
}

void ScanBase::incOutputTime(int64_t time) {
    _scanInfo.set_totaloutputtime(_scanInfo.totaloutputtime() + time);
}

void ScanBase::incTotalTime(int64_t time) {
    _scanInfo.set_totalusetime(_scanInfo.totalusetime() + time);
}
void ScanBase::incTotalScanCount(int64_t count) {
    _scanInfo.set_totalscancount(_scanInfo.totalscancount() + count);
}
void ScanBase::setTotalSeekCount(int64_t count) {
    _scanInfo.set_totalseekcount(count);
}
void ScanBase::incTotalOutputCount(int64_t count) {
    _scanInfo.set_totaloutputcount(_scanInfo.totaloutputcount() + count);
}
void ScanBase::updateDurationTime(int64_t time) {
    _scanInfo.set_durationtime(time);
}

void ScanBase::updateExtraInfo(std::string info) {
    _scanInfo.set_extrainfo(std::move(info));
}

} // namespace sql
} // namespace isearch
