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
#include "ha3/sql/ops/scan/KVScan.h"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <stdint.h>
#include <unordered_set>
#include <utility>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/ConstString.h"
#include "autil/DataBuffer.h"
#include "autil/MultiValueCreator.h"
#include "autil/MultiValueType.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/Pool.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/sql/ops/calc/CalcTable.h"
#include "ha3/sql/ops/condition/Condition.h"
#include "ha3/sql/ops/condition/ConditionParser.h"
#include "ha3/sql/ops/condition/ConditionVisitor.h"
#include "ha3/sql/ops/scan/AsyncKVLookupCallbackCtxV1.h"
#include "ha3/sql/ops/scan/AsyncKVLookupCallbackCtxV2.h"
#include "ha3/sql/ops/scan/Collector.h"
#include "ha3/sql/ops/scan/PrimaryKeyScanConditionVisitor.h"
#include "ha3/sql/ops/scan/ScanBase.h"
#include "ha3/sql/ops/scan/ScanResource.h"
#include "ha3/sql/ops/scan/ScanUtil.h"
#include "ha3/sql/proto/SqlSearchInfo.pb.h"
#include "ha3/sql/resource/TabletManagerR.h"
#include "indexlib/config/legacy_schema_adapter.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/index/kv/kv_define.h"
#include "indexlib/index/kv/kv_index_options.h"
#include "indexlib/index/kv/kv_reader.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/table/BuiltinDefine.h"
#include "indexlib/index/kv/KVIndexReader.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/table/kv_table/KVTabletSessionReader.h"
#include "matchdoc/CommonDefine.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Reference.h"
#include "matchdoc/Trait.h"
#include "matchdoc/ValueType.h"
#include "navi/resource/GraphMemoryPoolResource.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/util/IndexInfos.h"
#include "suez/turing/expression/util/TableInfo.h"
#include "table/Table.h"
#include "table/ValueTypeSwitch.h"

using namespace std;
using namespace autil;
using namespace table;
using namespace matchdoc;
using namespace suez::turing;
using namespace indexlibv2::index;
using namespace isearch::common;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, KVScan);

KVScan::KVScan(const ScanInitParam &param, navi::AsyncPipePtr pipe, bool requirePk)
    : ScanBase(param)
    , _asyncPipe(pipe)
    , _requirePk(requirePk)
    , _canReuseAllocator(false) {}

KVScan::~KVScan() {}

bool KVScan::doInit() {
    _canReuseAllocator = _scanOnce;
    auto &scanResource = _param.scanResource;
    if (scanResource.metricsReporter != nullptr) {
        _opMetricsReporter = scanResource.metricsReporter;
    }
    if (!prepareIndexInfo()) {
        SQL_LOG(ERROR, "prepare indexInfo failed");
        return false;
    }
    if (!prepareLookUpCtx()) {
        SQL_LOG(ERROR, "prepare look up ctx failed");
        return false;
    }
    // init pk query
    if (!parseQuery()) {
        SQL_LOG(ERROR, "parse query failed");
        return false;
    }

    // prepare attr && _table declare column
    if (!prepareFields()) {
        SQL_LOG(ERROR, "prepare fields failed");
        return false;
    }
    startLookupCtx();
    // init calc
    CalcInitParam calcInitParam(_param.outputFields,
                                _param.outputFieldsType,
                                _param.conditionJson,
                                _param.outputExprsJson);
    CalcResource calcResource;
    calcResource.memoryPoolResource = scanResource.memoryPoolResource;
    calcResource.tracer = scanResource.tracer;
    calcResource.cavaAllocator = scanResource.suezCavaAllocator;
    calcResource.cavaPluginManager = scanResource.cavaPluginManager;
    calcResource.funcInterfaceCreator = scanResource.functionInterfaceCreator;
    calcResource.metricsReporter = scanResource.metricsReporter;
    _calcTable.reset(new CalcTable(calcInitParam, std::move(calcResource)));
    if (!_calcTable->init()) {
        SQL_LOG(ERROR, "calc table init failed");
        return false;
    }
    return true;
}

bool KVScan::doBatchScan(TablePtr &table, bool &eof) {
    incSeekTime(_lookupCtx->getSeekTime());

    autil::ScopedTime2 outputTimer;
    std::vector<matchdoc::MatchDoc> matchDocs;
    if (_matchDocAllocator == nullptr) {
        SQL_LOG(ERROR, "prepare matchdoc allocator failed");
        return false;
    }
    if (!fillMatchDocs(matchDocs)) {
        SQL_LOG(ERROR, "fill kv failed");
        return false;
    }
    _scanInfo.set_totalscancount(_scanInfo.totalscancount() + matchDocs.size());
    table = createTable(matchDocs, _matchDocAllocator, _canReuseAllocator);
    // clear batch pool
    _matchDocAllocator.reset();
    incOutputTime(outputTimer.done_us());

    autil::ScopedTime2 evaluteTimer;
    if (!_calcTable->calcTable(table)) {
        SQL_LOG(ERROR, "calc table failed");
        return false;
    }
    _seekCount += table->getRowCount();
    incEvaluateTime(evaluteTimer.done_us());

    outputTimer = {};
    if (_limit < _seekCount) {
        table->clearBackRows(min(_seekCount - _limit, (uint32_t)table->getRowCount()));
    }
    incOutputTime(outputTimer.done_us());

    eof = true;
    return true;
}

bool KVScan::doUpdateScanQuery(const StreamQueryPtr &inputQuery) {
    _scanOnce = false;
    _canReuseAllocator = true;
    _rawPks.clear();
    if (!inputQuery) {
        SQL_LOG(TRACE3, "raw pk is empty for null input query");
    } else {
        std::unordered_set<std::string> keySet;
        const std::vector<std::string> &pks = inputQuery->primaryKeys;
        for (size_t i = 0; i < pks.size(); ++i) {
            const std::string &pk = pks[i];
            auto pair = keySet.insert(pk);
            if (pair.second) {
                _rawPks.emplace_back(pk);
            }
        }
        ScanUtil::filterPksByParam(_param, _indexInfo, _rawPks);
    }
    if (!prepareMatchDocAllocator()) {
        SQL_LOG(ERROR, "prepare batch pool failed");
        return false;
    }
    if (!prepareFields()) {
        SQL_LOG(ERROR, "prepare fields failed");
        return false;
    }
    auto option = prepareLookupOption();
    _lookupCtx->start(_rawPks, std::move(option));
    return true;
}

bool KVScan::prepareIndexInfo() {
    if (!_tableInfo) {
        SQL_LOG(ERROR, "table name [%s], table info is null", _param.tableName.c_str());
        return false;
    }
    _indexInfo = _tableInfo->getPrimaryKeyIndexInfo();
    if (!_indexInfo) {
        SQL_LOG(ERROR, "table name [%s], pk index info is null", _param.tableName.c_str());
        return false;
    }
    _indexName = _indexInfo->indexName;
    return true;
}

bool KVScan::parseQuery() {
    ConditionParser parser(_param.scanResource.kernelPoolPtr.get());
    ConditionPtr condition;
    if (!parser.parseCondition(_param.conditionJson, condition)) {
        SQL_LOG(ERROR,
                "table name [%s], parse condition [%s] failed",
                _param.tableName.c_str(),
                _param.conditionJson.c_str());
        return false;
    }
    if (condition == nullptr) {
        return !_requirePk;
    }
    KVScanConditionVisitor visitor(_indexInfo->fieldName, _indexInfo->indexName);
    condition->accept(&visitor);
    CHECK_VISITOR_ERROR(visitor);
    if (!visitor.stealHasQuery()) {
        if (_requirePk) {
            string errorMsg;
            if (condition) {
                errorMsg = "condition: " + condition->toString();
            } else {
                errorMsg = "condition json: " + _param.conditionJson;
            }
            SQL_LOG(ERROR,
                    "kv table name [%s] need key query, %s",
                    _param.tableName.c_str(),
                    errorMsg.c_str());
        }
        return !_requirePk;
    }
    _rawPks = visitor.stealRawKeyVec();
    SQL_LOG(TRACE3, "before filter rawPks[%s]", autil::StringUtil::toString(_rawPks).c_str());
    ScanUtil::filterPksByParam(_param, _indexInfo, _rawPks);
    SQL_LOG(TRACE3, "after filter rawPks[%s]", autil::StringUtil::toString(_rawPks).c_str());
    return true;
}

indexlib::index::KVReaderPtr KVScan::prepareKVReaderV1() {
    auto partReader = _param.scanResource.getPartitionReader(_param.tableName);
    if (!partReader) {
        SQL_LOG(TRACE3, "get partition reader v1 failed from table [%s]", _param.tableName.c_str());
        return nullptr;
    }
    auto schema = partReader->GetTabletSchema();
    if (!schema) {
        SQL_LOG(TRACE3, "get schema from reader v1 failed from table [%s]", _param.tableName.c_str());
        return nullptr;
    }
    if (schema->GetTableType() != indexlib::table::TABLE_TYPE_KV) {
        SQL_LOG(TRACE3, "table [%s] is not kv table", _param.tableName.c_str());
        return nullptr;
    }
    _schema = schema;
    return _indexPartitionReaderWrapper->getKVReader();
}

bool KVScan::prepareKvReaderV2Schema() {
    auto kvTabletReader = _param.scanResource.getTabletReader(_param.tableName).get();
    if (kvTabletReader == nullptr) {
        SQL_LOG(TRACE3, "kv table reader v2 not exist, table name [%s]", _param.tableName.c_str());
        return false;
    }
    auto tabSchemaPtr = kvTabletReader->GetSchema();
    if (!tabSchemaPtr) {
        SQL_LOG(TRACE3, "get table schema from reader v2 failed, table name [%s]", _param.tableName.c_str());
        return false;
    }
    if (tabSchemaPtr->GetTableType() != indexlib::table::TABLE_TYPE_KV &&
        tabSchemaPtr->GetTableType() != indexlib::table::TABLE_TYPE_AGGREGATION) {
        SQL_LOG(TRACE3, "table [%s] is not kv table", _param.tableName.c_str());
        return false;
    }
    _schema = tabSchemaPtr;
    return true;
}

bool KVScan::prepareLookUpCtx() {
    auto kvReader1 = prepareKVReaderV1();

    if (kvReader1) {
        _lookupCtx.reset(new AsyncKVLookupCallbackCtxV1(
                        _asyncPipe,
                        _param.scanResource.asyncIntraExecutor));
        _kvReader = std::move(kvReader1);
    } else if (_param.scanResource.tabletManagerR) {
        SQL_LOG(TRACE3, "table [%s] will get v2 reader in the future", _param.tableName.c_str());
        if (!prepareKvReaderV2Schema()) {
            SQL_LOG(ERROR, "table [%s] prepare kv reader v2 schema failed", _param.tableName.c_str());
            return false;
        }
        _lookupCtx.reset(
            new AsyncKVLookupCallbackCtxV2(_asyncPipe, _param.scanResource.asyncIntraExecutor));
    } else {
        SQL_LOG(ERROR, "table [%s] get kv reader failed", _param.tableName.c_str());
        return false;
    }
    _lookupCtx->setTracePtr(this);
    return true;
}

bool KVScan::prepareFields() {
    assert(_schema);
    KeyCollectorPtr pkeyCollector(new KeyCollector());
    if (!pkeyCollector->init(_schema, _matchDocAllocator)) {
        SQL_LOG(ERROR, "table [%s] KeyCollector init failed!", _param.tableName.c_str());
        return false;
    }
    ValueCollectorPtr valueCollector(new ValueCollector());
    auto legacySchemaAdapter
        = std::dynamic_pointer_cast<indexlib::config::LegacySchemaAdapter>(_schema);
    
    if (legacySchemaAdapter != nullptr) {
        if (!valueCollector->init(legacySchemaAdapter->GetLegacySchema(), _matchDocAllocator)) {
            SQL_LOG(ERROR, "table [%s] ValueCollector init failed!", _param.tableName.c_str());
            return false;
        }
    } else {
        auto indexConfig = _schema->GetIndexConfig(indexlib::index::KV_INDEX_TYPE_STR, _indexName);
        if (indexConfig == nullptr) {
            SQL_LOG(ERROR, "table [%s] get index [%s] config failed", _param.tableName.c_str(), _indexName.c_str());
            return false;
        }
        auto kvIndexConfig = dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(indexConfig);
        if (kvIndexConfig == nullptr) {
            SQL_LOG(ERROR, "get kv index config failed");
            return false;
        }
        if (!valueCollector->init(kvIndexConfig->GetValueConfig(), _matchDocAllocator, _schema->GetTableType())) {
            SQL_LOG(ERROR, "table [%s] ValueCollector init failed!", _param.tableName.c_str());
            return false;
        }
    }
    _pkeyCollector = pkeyCollector;
    _valueCollector = valueCollector;
    return true;
}

KVLookupOption KVScan::prepareLookupOption() {
    KVLookupOption option;
    option.leftTime = _timeoutTerminator ? _timeoutTerminator->getLeftTime() : numeric_limits<int64_t>::max();
    option.pool = _matchDocAllocator->getPoolPtr();
    option.maxConcurrency = _param.scanResource.scanConfig.asyncScanConcurrency;
    option.kvReader = _kvReader.get();
    if (_param.targetWatermarkType == WatermarkType::WM_TYPE_SYSTEM_TS && _param.targetWatermark == 0) {
        // use searcher timestamp
        option.targetWatermark = TimeUtility::currentTime();
    } else {
        option.targetWatermark =  _param.targetWatermark;
    }
    option.targetWatermarkType = _param.targetWatermarkType;
    option.targetWatermarkTimeoutRatio = _param.scanResource.scanConfig.targetWatermarkTimeoutRatio;
    option.indexName = _indexName;
    option.tableName = _param.tableName;
    option.tabletManagerR = _param.scanResource.tabletManagerR;
    return option;
}

void KVScan::startLookupCtx() {
    auto option = prepareLookupOption();
    _lookupCtx->start(_rawPks, std::move(option));
}

void KVScan::onBatchScanFinish() {
    if (_opMetricsReporter) {
        auto opMetricsReporter = _opMetricsReporter->getSubReporter(
            "", {{{"table_name", getTableNameForMetrics()}}});
        if (!_lookupCtx->tryReportMetrics(*opMetricsReporter)) {
            SQL_LOG(TRACE3, "ignore report metrics");
        }
    }
}

bool KVScan::fillMatchDocs(std::vector<matchdoc::MatchDoc> &matchDocs) {
    if (!_pkeyCollector || !_valueCollector || !_lookupCtx) {
        return false;
    }
    if (_scanInfo.buildwatermark() == 0) {
        _scanInfo.set_buildwatermark(_lookupCtx->getBuildWatermark());
    }
    _scanInfo.set_waitwatermarktime(_scanInfo.waitwatermarktime() + _lookupCtx->getWaitWatermarkTime());
    _scanInfo.set_degradeddocscount(_scanInfo.degradeddocscount() + _lookupCtx->getDegradeDocsSize());
    NAVI_KERNEL_LOG(TRACE3, "lookup desc: %s", _lookupCtx->getLookupDesc().c_str());

    auto &results = _lookupCtx->getResults();
    matchDocs.clear();
    matchDocs.reserve(results.size());

    auto func = [&](auto a) {
        typedef typename decltype(a)::value_type T;
        if constexpr(std::is_same_v<T, bool>) {
            assert(false && "bool type pk is not supported");
        }
        auto *pkeyRef = dynamic_cast<Reference<T> *>(_pkeyCollector->getPkeyRef());
        T typedPk;
        for (size_t i = 0; i < results.size(); ++i) {
            if (!results[i]) {
                // get failed, skip
                continue;
            }
            if (!autil::StringUtil::fromString<T>(_rawPks[i], typedPk)) {
                return false;
            }
            MatchDoc doc = _matchDocAllocator->allocate();
            if (NULL != pkeyRef) {
                pkeyRef->set(doc, typedPk);
            }
            _valueCollector->collectFields(doc, *results[i]);
            matchDocs.push_back(doc);
        }
        return true;
    };
    auto stringFunc = [&]() {
        auto *pkeyRef = dynamic_cast<Reference<autil::MultiChar> *>(_pkeyCollector->getPkeyRef());
        autil::StringView typedPk;
        for (size_t i = 0; i < results.size(); ++i) {
            if (!results[i]) {
                // get failed, skip
                continue;
            }
            if (!autil::StringUtil::fromString(_rawPks[i], typedPk)) {
                return false;
            }
            MatchDoc doc = _matchDocAllocator->allocate();
            if (NULL != pkeyRef) {
                pkeyRef->set(doc, typedPk);
            }
            _valueCollector->collectFields(doc, *results[i]);
            matchDocs.push_back(doc);
        }
        return true;
    };
    auto invalidFunc = [&](auto a) {
        typedef typename decltype(a)::value_type T;
        SQL_LOG(ERROR, "table [%s] unsupported pkey type [%s]",
                _param.tableName.c_str(), typeid(T).name());
        return false;
    };
    auto invalidFunc2 = [&]() {
        SQL_LOG(ERROR, "table [%s] unsupported pkey type [%s]",
                _param.tableName.c_str(), typeid(autil::MultiString).name());
        return false;
    };
    if (!ValueTypeSwitch::switchType(
                    _pkeyCollector->getPkeyBuiltinType(), false,
                    func, invalidFunc, stringFunc, invalidFunc2)) {
        SQL_LOG(ERROR, "fill matchdoc failed");
        return false;
    }
    return true;
}

} // namespace sql
} // namespace isearch
