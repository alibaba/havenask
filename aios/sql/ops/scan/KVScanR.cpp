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
#include "sql/ops/scan/KVScanR.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <engine/NaviConfigContext.h>
#include <memory>
#include <stdint.h>
#include <typeinfo>
#include <unordered_set>
#include <utility>

#include "autil/MultiValueType.h"
#include "autil/Span.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/TimeoutTerminator.h"
#include "autil/mem_pool/Pool.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/config/legacy_schema_adapter.h"
#include "indexlib/framework/ITabletReader.h"
#include "indexlib/index/attribute/Constant.h"
#include "indexlib/index/kv/Common.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/table/BuiltinDefine.h"
#include "kmonitor/client/MetricsReporter.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Reference.h"
#include "matchdoc/VectorDocStorage.h"
#include "navi/builder/ResourceDefBuilder.h"
#include "navi/proto/KernelDef.pb.h"
#include "sql/common/Log.h"
#include "sql/common/WatermarkType.h"
#include "sql/framework/PushDownOp.h"
#include "sql/ops/calc/CalcInitParamR.h"
#include "sql/ops/condition/Condition.h"
#include "sql/ops/condition/ConditionParser.h"
#include "sql/ops/condition/ConditionVisitor.h"
#include "sql/ops/scan/AsyncKVLookupCallbackCtx.h"
#include "sql/ops/scan/AsyncKVLookupCallbackCtxV1.h"
#include "sql/ops/scan/AsyncKVLookupCallbackCtxV2.h"
#include "sql/ops/scan/Collector.h"
#include "sql/ops/scan/PrimaryKeyScanConditionVisitor.h"
#include "sql/ops/scan/ScanBase.h"
#include "sql/ops/scan/ScanR.h"
#include "sql/ops/scan/ScanUtil.h"
#include "sql/proto/SqlSearchInfo.pb.h"
#include "sql/resource/QueryMetricReporterR.h"
#include "sql/resource/TabletManagerR.h"
#include "sql/resource/TimeoutTerminatorR.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/util/IndexInfos.h"
#include "suez/turing/expression/util/TableInfo.h"
#include "table/Table.h"
#include "table/ValueTypeSwitch.h"

namespace navi {
class ResourceInitContext;
} // namespace navi

using namespace std;
using namespace autil;
using namespace table;
using namespace matchdoc;
using namespace suez::turing;
using namespace indexlibv2::index;
using namespace isearch::common;

namespace sql {

const std::string KVScanR::RESOURCE_ID = "kv_scan_r";

KVScanR::KVScanR() {}

KVScanR::~KVScanR() {}

void KVScanR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_KERNEL);
}

bool KVScanR::config(navi::ResourceConfigContext &ctx) {
    NAVI_JSONIZE(ctx, "kv_require_pk", _requirePk, _requirePk);
    NAVI_JSONIZE(ctx, "kv_async", _asyncMode, _asyncMode);
    return true;
}

navi::ErrorCode KVScanR::init(navi::ResourceInitContext &ctx) {
    if (!ScanBase::doInit()) {
        return navi::EC_ABORT;
    }
    if (_asyncMode) {
        auto asyncPipe = _watermarkR->getAsyncPipe();
        if (asyncPipe) {
            _enableWatermark = true;
            setAsyncPipe(asyncPipe);
        } else if (!initAsyncPipe(ctx)) {
            return navi::EC_ABORT;
        }
    }
    _canReuseAllocator = _scanOnce;
    if (!prepareIndexInfo()) {
        SQL_LOG(ERROR, "prepare indexInfo failed");
        return navi::EC_ABORT;
    }
    if (!prepareLookUpCtx()) {
        SQL_LOG(ERROR, "prepare look up ctx failed");
        return navi::EC_ABORT;
    }
    // init pk query
    if (!parseQuery()) {
        SQL_LOG(ERROR, "parse query failed");
        return navi::EC_ABORT;
    }

    // prepare attr && _table declare column
    if (!prepareFields()) {
        SQL_LOG(ERROR, "prepare fields failed");
        return navi::EC_ABORT;
    }
    if (!_enableWatermark) {
        startAsyncLookup();
    }
    return navi::EC_NONE;
}

bool KVScanR::doBatchScan(TablePtr &table, bool &eof) {
    _scanInitParamR->incSeekTime(_lookupCtx->getSeekTime() + _watermarkR->getWaitWatermarkTime());

    autil::ScopedTime2 outputTimer;
    std::vector<matchdoc::MatchDoc> matchDocs;
    if (!fillMatchDocs(matchDocs)) {
        SQL_LOG(ERROR, "fill kv failed");
        return false;
    }
    _scanCount += matchDocs.size();
    table = createTable(
        matchDocs, _attributeExpressionCreatorR->_matchDocAllocator, _canReuseAllocator);
    // clear batch pool
    _attributeExpressionCreatorR->initMatchDocAllocator();
    if (!prepareFields()) {
        SQL_LOG(ERROR, "prepare fields failed");
        return false;
    }
    _scanInitParamR->incOutputTime(outputTimer.done_us());
    autil::ScopedTime2 evaluteTimer;
    if (!_calcTableR->calcTable(table)) {
        SQL_LOG(ERROR, "calc table failed");
        return false;
    }
    _scanInitParamR->incEvaluateTime(evaluteTimer.done_us());

    outputTimer = {};
    if (size_t rowCount = table->getRowCount(); _limit < rowCount) {
        table->clearBackRows(rowCount - _limit);
    }
    _scanInitParamR->incOutputTime(outputTimer.done_us());

    eof = true;
    return true;
}

bool KVScanR::doUpdateScanQuery(const StreamQueryPtr &inputQuery) {
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
        ScanUtil::filterPksByParam(_scanR, *_scanInitParamR, _indexInfo, _rawPks);
    }
    startAsyncLookup();
    return true;
}

bool KVScanR::prepareIndexInfo() {
    const auto &tableInfo = _attributeExpressionCreatorR->_tableInfo;
    if (!tableInfo) {
        SQL_LOG(ERROR, "table name [%s], table info is null", _scanInitParamR->tableName.c_str());
        return false;
    }
    _indexInfo = tableInfo->getPrimaryKeyIndexInfo();
    if (!_indexInfo) {
        SQL_LOG(
            ERROR, "table name [%s], pk index info is null", _scanInitParamR->tableName.c_str());
        return false;
    }
    _indexName = _indexInfo->indexName;
    return true;
}

bool KVScanR::parseQuery() {
    ConditionParser parser(_scanR->kernelPoolPtr.get());
    ConditionPtr condition;
    const auto &conditionJson = _scanInitParamR->calcInitParamR->conditionJson;
    if (!parser.parseCondition(conditionJson, condition)) {
        SQL_LOG(ERROR,
                "table name [%s], parse condition [%s] failed",
                _scanInitParamR->tableName.c_str(),
                conditionJson.c_str());
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
                errorMsg = "condition json: " + conditionJson;
            }
            SQL_LOG(ERROR,
                    "kv table name [%s] need key query, %s",
                    _scanInitParamR->tableName.c_str(),
                    errorMsg.c_str());
        }
        return !_requirePk;
    }
    _rawPks = visitor.stealRawKeyVec();
    SQL_LOG(TRACE3, "before filter rawPks[%s]", autil::StringUtil::toString(_rawPks).c_str());
    ScanUtil::filterPksByParam(_scanR, *_scanInitParamR, _indexInfo, _rawPks);
    SQL_LOG(TRACE3, "after filter rawPks[%s]", autil::StringUtil::toString(_rawPks).c_str());
    return true;
}

indexlib::index::KVReaderPtr KVScanR::prepareKVReaderV1() {
    auto partReader = _scanR->getPartitionReader(_scanInitParamR->tableName);
    if (!partReader) {
        SQL_LOG(TRACE3,
                "get partition reader v1 failed from table [%s]",
                _scanInitParamR->tableName.c_str());
        return nullptr;
    }
    auto schema = partReader->GetTabletSchema();
    if (!schema) {
        SQL_LOG(TRACE3,
                "get schema from reader v1 failed from table [%s]",
                _scanInitParamR->tableName.c_str());
        return nullptr;
    }
    if (schema->GetTableType() != indexlib::table::TABLE_TYPE_KV) {
        SQL_LOG(TRACE3, "table [%s] is not kv table", _scanInitParamR->tableName.c_str());
        return nullptr;
    }
    _schema = schema;
    return _attributeExpressionCreatorR->_indexPartitionReaderWrapper->getKVReader();
}

bool KVScanR::prepareKvReaderV2Schema() {
    auto kvTabletReader = _scanR->getTabletReader(_scanInitParamR->tableName).get();
    if (kvTabletReader == nullptr) {
        SQL_LOG(TRACE3,
                "kv table reader v2 not exist, table name [%s]",
                _scanInitParamR->tableName.c_str());
        return false;
    }
    auto tabSchemaPtr = kvTabletReader->GetSchema();
    if (!tabSchemaPtr) {
        SQL_LOG(TRACE3,
                "get table schema from reader v2 failed, table name [%s]",
                _scanInitParamR->tableName.c_str());
        return false;
    }
    if (tabSchemaPtr->GetTableType() != indexlib::table::TABLE_TYPE_KV
        && tabSchemaPtr->GetTableType() != indexlib::table::TABLE_TYPE_AGGREGATION) {
        SQL_LOG(TRACE3, "table [%s] is not kv table", _scanInitParamR->tableName.c_str());
        return false;
    }
    _schema = tabSchemaPtr;
    return true;
}

bool KVScanR::prepareLookUpCtx() {
    auto kvReader1 = prepareKVReaderV1();

    const auto &asyncPipe = getAsyncPipe();
    if (kvReader1) {
        _lookupCtx.reset(
            new AsyncKVLookupCallbackCtxV1(asyncPipe, _asyncExecutorR->getAsyncIntraExecutor()));
        _kvReader = std::move(kvReader1);
    } else {
        SQL_LOG(TRACE3,
                "table [%s] will get v2 reader in the future",
                _scanInitParamR->tableName.c_str());
        if (!prepareKvReaderV2Schema()) {
            SQL_LOG(ERROR,
                    "table [%s] prepare kv reader v2 schema failed",
                    _scanInitParamR->tableName.c_str());
            return false;
        }
        if (!_watermarkR->getTablet()) {
            SQL_LOG(ERROR, "tablet [%s] not exist", _scanInitParamR->tableName.c_str());
            return false;
        }
        _lookupCtx.reset(
            new AsyncKVLookupCallbackCtxV2(asyncPipe, _asyncExecutorR->getAsyncIntraExecutor()));
    }
    _lookupCtx->setTracePtr(this);
    return true;
}

bool KVScanR::prepareFields() {
    const auto &matchDocAllocator = _attributeExpressionCreatorR->_matchDocAllocator;
    assert(_schema);
    KeyCollectorPtr pkeyCollector(new KeyCollector());
    if (!pkeyCollector->init(_schema, matchDocAllocator)) {
        SQL_LOG(ERROR, "table [%s] KeyCollector init failed!", _scanInitParamR->tableName.c_str());
        return false;
    }
    ValueCollectorPtr valueCollector(new ValueCollector());
    auto legacySchemaAdapter
        = std::dynamic_pointer_cast<indexlib::config::LegacySchemaAdapter>(_schema);

    if (legacySchemaAdapter != nullptr) {
        if (!valueCollector->init(legacySchemaAdapter->GetLegacySchema(), matchDocAllocator)) {
            SQL_LOG(ERROR,
                    "table [%s] ValueCollector init failed!",
                    _scanInitParamR->tableName.c_str());
            return false;
        }
    } else {
        auto indexConfig = _schema->GetIndexConfig(indexlib::index::KV_INDEX_TYPE_STR, _indexName);
        if (indexConfig == nullptr) {
            SQL_LOG(ERROR,
                    "table [%s] get index [%s] config failed",
                    _scanInitParamR->tableName.c_str(),
                    _indexName.c_str());
            return false;
        }
        auto kvIndexConfig = dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(indexConfig);
        if (kvIndexConfig == nullptr) {
            SQL_LOG(ERROR, "get kv index config failed");
            return false;
        }
        if (!valueCollector->init(
                kvIndexConfig->GetValueConfig(), matchDocAllocator, _schema->GetTableType())) {
            SQL_LOG(ERROR,
                    "table [%s] ValueCollector init failed!",
                    _scanInitParamR->tableName.c_str());
            return false;
        }
    }
    _pkeyCollector = pkeyCollector;
    _valueCollector = valueCollector;
    return true;
}

KVLookupOption KVScanR::prepareLookupOption() {
    KVLookupOption option;
    option.leftTime = _timeoutTerminatorR->getTimeoutTerminator()->getLeftTime();
    option.pool = _attributeExpressionCreatorR->_matchDocAllocator->getPoolPtr();
    option.maxConcurrency = _scanR->scanConfig.asyncScanConcurrency;
    option.kvReader = _kvReader.get();
    option.indexName = _indexName;
    option.tableName = _scanInitParamR->tableName;
    option.tablet = _watermarkR->getTablet();
    return option;
}

void KVScanR::startAsyncLookup() {
    auto option = prepareLookupOption();
    _lookupCtx->start(_rawPks, std::move(option));
}

void KVScanR::onBatchScanFinish() {
    if (_queryMetricReporterR) {
        auto opMetricsReporter = _queryMetricReporterR->getReporter()->getSubReporter(
            "", {{{"table_name", getTableNameForMetrics()}}});
        if (!_lookupCtx->tryReportMetrics(*opMetricsReporter)) {
            SQL_LOG(TRACE3, "ignore report metrics");
        }
        _watermarkR->tryReportMetrics(*opMetricsReporter);
    }
}

bool KVScanR::fillMatchDocs(std::vector<matchdoc::MatchDoc> &matchDocs) {
    if (!_pkeyCollector || !_valueCollector || !_lookupCtx) {
        return false;
    }
    auto &scanInfo = _scanInitParamR->scanInfo;
    if (scanInfo.buildwatermark() == 0) {
        scanInfo.set_buildwatermark(_watermarkR->getBuildWatermark());
    }
    bool waitFailed = _watermarkR->waitFailed();
    if (waitFailed || _lookupCtx->getFailedCount() > 0) {
        if (!_queryConfig->resultallowsoftfailure()) {
            SQL_LOG(ERROR,
                    "search degrade is not allowed by query config, abort. wait watermark[%s], "
                    "failed docs[%lu]",
                    waitFailed ? "failed" : "success",
                    _lookupCtx->getFailedCount());
            return false;
        }
        DegradedInfo degradedInfo;
        degradedInfo.add_degradederrorcodes(INCOMPLETE_DATA_ERROR);
        if (_sqlSearchInfoCollectorR) {
            _sqlSearchInfoCollectorR->getCollector()->addDegradedInfo(std::move(degradedInfo));
        }
    }
    scanInfo.set_waitwatermarktime(scanInfo.waitwatermarktime()
                                   + _watermarkR->getWaitWatermarkTime());
    auto degradeDocsCount = waitFailed ? _rawPks.size() : _lookupCtx->getFailedCount();
    scanInfo.set_degradeddocscount(scanInfo.degradeddocscount() + degradeDocsCount);
    SQL_LOG(TRACE3, "lookup desc: %s", _lookupCtx->getLookupDesc().c_str());

    if (!_lookupCtx->isSchemaMatch(_schema)) {
        SQL_LOG(ERROR, "schema not match, abort");
        return false;
    }
    auto &results = _lookupCtx->getResults();
    matchDocs.clear();
    matchDocs.reserve(results.size());

    auto func = [&](auto a) {
        typedef typename decltype(a)::value_type T;
        if constexpr (std::is_same_v<T, bool>) {
            assert(false && "bool type pk is not supported");
        }
        auto *pkeyRef = dynamic_cast<Reference<T> *>(_pkeyCollector->getPkeyRef());
        T typedPk;
        if constexpr (std::is_same_v<T, autil::HllCtx>) {
            return false;
        } else {
            for (size_t i = 0; i < results.size(); ++i) {
                if (!results[i]) {
                    // get failed, skip
                    continue;
                }
                if (!autil::StringUtil::fromString<T>(_rawPks[i], typedPk)) {
                    return false;
                }
                MatchDoc doc = _attributeExpressionCreatorR->_matchDocAllocator->allocate();
                if (NULL != pkeyRef) {
                    pkeyRef->set(doc, typedPk);
                }
                _valueCollector->collectFields(doc, *results[i]);
                matchDocs.push_back(doc);
            }
            return true;
        }
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
            MatchDoc doc = _attributeExpressionCreatorR->_matchDocAllocator->allocate();
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
        SQL_LOG(ERROR,
                "table [%s] unsupported pkey type [%s]",
                _scanInitParamR->tableName.c_str(),
                typeid(T).name());
        return false;
    };
    auto invalidFunc2 = [&]() {
        SQL_LOG(ERROR,
                "table [%s] unsupported pkey type [%s]",
                _scanInitParamR->tableName.c_str(),
                typeid(autil::MultiString).name());
        return false;
    };
    if (!ValueTypeSwitch::switchType(_pkeyCollector->getPkeyBuiltinType(),
                                     false,
                                     func,
                                     invalidFunc,
                                     stringFunc,
                                     invalidFunc2)) {
        SQL_LOG(ERROR, "fill matchdoc failed");
        return false;
    }
    return true;
}

REGISTER_RESOURCE(KVScanR);

} // namespace sql
