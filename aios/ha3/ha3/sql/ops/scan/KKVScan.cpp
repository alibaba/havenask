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
#include "ha3/sql/ops/scan/KKVScan.h"

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
#include "ha3/sql/ops/scan/Collector.h"
#include "ha3/sql/ops/scan/PrimaryKeyScanConditionVisitor.h"
#include "ha3/sql/ops/scan/ScanBase.h"
#include "ha3/sql/proto/SqlSearchInfo.pb.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/legacy_schema_adapter.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/index/kkv/kkv_iterator.h"
#include "indexlib/index/kkv/kkv_reader.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/indexlib.h"
#include "indexlib/table/BuiltinDefine.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/util/PoolUtil.h"
#include "matchdoc/CommonDefine.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Reference.h"
#include "matchdoc/Trait.h"
#include "matchdoc/ValueType.h"
#include "navi/resource/GraphMemoryPoolResource.h"
#include "suez/turing/expression/common.h"
#include "table/Table.h"

using namespace std;
using namespace autil;
using namespace table;
using namespace matchdoc;
using namespace suez::turing;
using namespace indexlib;
using namespace indexlib::index;
using namespace indexlib::config;
using namespace isearch::common;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, KKVScan);

KKVScan::KKVScan(const ScanInitParam &param, bool requirePk)
    : ScanBase(param)
    , _requirePk(requirePk)
    , _canReuseAllocator(false) {}

KKVScan::~KKVScan() {}

bool KKVScan::doInit() {
    _canReuseAllocator = _scanOnce;
    const ScanResource &scanResource = _param.scanResource;
    if (!prepareKKVReader()) {
        SQL_LOG(ERROR, "prepare kkv reader failed");
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

bool KKVScan::doBatchScan(TablePtr &table, bool &eof) {
    _scanInfo.set_totalscancount(_scanInfo.totalscancount() + _rawPks.size());

    autil::ScopedTime2 seekTimer;
    std::vector<matchdoc::MatchDoc> matchDocs;
    if (!fillMatchDocs(matchDocs)) {
        SQL_LOG(ERROR, "fill kkv failed");
        return false;
    }
    incSeekTime(seekTimer.done_us());

    autil::ScopedTime2 outputTimer;
    table = createTable(matchDocs, _matchDocAllocator, _canReuseAllocator);
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

bool KKVScan::doUpdateScanQuery(const StreamQueryPtr &inputQuery) {
    _scanOnce = false;
    _canReuseAllocator = true;
    _rawPks.clear();
    if (!inputQuery) {
        return true;
    }
    std::unordered_set<std::string> keySet;
    const std::vector<std::string> &pks = inputQuery->primaryKeys;
    std::vector<std::string> unique_pks;
    for (size_t i = 0; i < pks.size(); ++i) {
        const std::string &pk = pks[i];
        auto pair = keySet.insert(pk);
        if (pair.second) {
            unique_pks.emplace_back(pk);
        }
    }
    filterPks(unique_pks);
    return true;
}

bool KKVScan::parseQuery() {
    ConditionParser parser(_param.scanResource.kernelPoolPtr.get());
    ConditionPtr condition;
    if (!parser.parseCondition(_param.conditionJson, condition)) {
        SQL_LOG(ERROR,
                "table name [%s], parse condition [%s] failed",
                _param.tableName.c_str(),
                _param.conditionJson.c_str());
        return false;
    }
    if (nullptr == condition) {
        return !_requirePk;
    }
    IndexSchemaPtr indexSchema = _schema->GetLegacySchema()->GetIndexSchema();
    SingleFieldIndexConfigPtr pkIndexConfig = indexSchema->GetPrimaryKeyIndexConfig();
    if (unlikely(nullptr == pkIndexConfig)) {
        return false;
    }
    KKVScanConditionVisitor visitor(indexSchema->GetPrimaryKeyIndexFieldName(),
                                    pkIndexConfig->GetIndexName());
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
                    "kkv table name [%s] need key query, %s",
                    _param.tableName.c_str(),
                    errorMsg.c_str());
        }
        return !_requirePk;
    }
    filterPks(visitor.getRawKeyVec());
    return true;
}

void KKVScan::filterPks(const std::vector<std::string> &pks) {
    size_t size = pks.size();
    _rawPks.reserve(size / _param.parallelNum + 1);
    for (size_t i = _param.parallelIndex; i < size; i += _param.parallelNum) {
        _rawPks.emplace_back(pks[i]);
    }
}

bool KKVScan::prepareKKVReader() {
    const string &tableName = _param.tableName;
    auto schema = _indexPartitionReaderWrapper->getSchema();
    if (!schema) {
        SQL_LOG(ERROR, "get schema failed from table [%s]", tableName.c_str());
        return false;
    }
    if (schema->GetTableType() != indexlib::table::TABLE_TYPE_KKV) {
        SQL_LOG(ERROR, "table [%s] is not kkv table", tableName.c_str());
        return false;
    }
    _schema = std::dynamic_pointer_cast<indexlib::config::LegacySchemaAdapter>(schema);
    if (!_schema) {
        // indexlibv2 does not support kkv yet
        SQL_LOG(ERROR, "schema[%s] cast to LegacySchemaAdapter fail", schema->GetTableName().c_str());
        return false;
    }

    _kkvReader = _indexPartitionReaderWrapper->getKKVReader();
    if (!_kkvReader) {
        SQL_LOG(ERROR, "table [%s] get kkv reader failed", tableName.c_str());
        return false;
    }
    return true;
}

bool KKVScan::prepareFields() {
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
        // indexlibv2 does not support kkv yet
        SQL_LOG(ERROR, "schema[%s] cast to LegacySchemaAdapter fail", _schema->GetTableName().c_str());
        return false;
    }

    _pkeyCollector = pkeyCollector;
    _valueCollector = valueCollector;
    return true;
}

bool KKVScan::fillMatchDocs(std::vector<matchdoc::MatchDoc> &matchDocs) {
    if (!_pkeyCollector || !_valueCollector || !_kkvReader) {
        return false;
    }
    matchDocs.reserve(_rawPks.size());
    int64_t timestamp = TimeUtility::currentTime();

#define NUM_PKEY_CASE_CLAUSE(key_t)                                                                \
    case key_t: {                                                                                  \
        if (unlikely(!_kkvReader->GetKKVIndexConfig()->UseNumberHash())) {                         \
            SQL_LOG(ERROR, "table [%s] pkey type is not-number", _param.tableName.c_str());        \
            return false;                                                                          \
        }                                                                                          \
        typedef MatchDocBuiltinType2CppType<key_t, false>::CppType T;                              \
        auto pkeyRef = dynamic_cast<Reference<T> *>(_pkeyCollector->getPkeyRef());                 \
        auto deleter = [this](indexlib::index::KKVIterator *p) {                                   \
            IE_POOL_COMPATIBLE_DELETE_CLASS(_param.scanResource.queryPool, p);                     \
        };                                                                                         \
        for (const string &pkeyStr : _rawPks) {                                                    \
            unique_ptr<KKVIterator, decltype(deleter)> kkvIter(nullptr, deleter);                  \
            T pkey;                                                                                \
            if (!autil::StringUtil::fromString(pkeyStr, pkey)) {                                   \
                return false;                                                                      \
            }                                                                                      \
            kkvIter.reset(_kkvReader->Lookup(                                                      \
                (PKeyType)pkey, timestamp, tsc_default, _param.scanResource.queryPool, nullptr));  \
            if (nullptr == kkvIter) {                                                              \
                SQL_LOG(WARN, "kkv iterator is nullptr, for pk [%s]", pkeyStr.c_str());            \
                continue;                                                                          \
            }                                                                                      \
            MatchDoc curMatchDoc = INVALID_MATCHDOC;                                               \
            int matchDocCount = 0;                                                                 \
            CollectorUtil::moveToNextVaildDoc(                                                     \
                kkvIter.get(), curMatchDoc, _matchDocAllocator, _valueCollector, matchDocCount);   \
            while (CollectorUtil::isVaildMatchDoc(curMatchDoc)) {                                  \
                matchDocs.push_back(curMatchDoc);                                                  \
                if (nullptr != pkeyRef) {                                                          \
                    pkeyRef->set(curMatchDoc, pkey);                                               \
                }                                                                                  \
                CollectorUtil::moveToNextVaildDoc(kkvIter.get(),                                   \
                                                  curMatchDoc,                                     \
                                                  _matchDocAllocator,                              \
                                                  _valueCollector,                                 \
                                                  matchDocCount);                                  \
            }                                                                                      \
            kkvIter->Finish();                                                                     \
        }                                                                                          \
        return true;                                                                               \
    }
    switch (_pkeyCollector->getPkeyBuiltinType()) {
        NUMBER_BUILTIN_TYPE_MACRO_HELPER(NUM_PKEY_CASE_CLAUSE);
    case bt_string: {
        Reference<autil::MultiChar> *pkeyRef
            = dynamic_cast<Reference<autil::MultiChar> *>(_pkeyCollector->getPkeyRef());
        auto deleter = [this](indexlib::index::KKVIterator *p) {
            IE_POOL_COMPATIBLE_DELETE_CLASS(_param.scanResource.queryPool, p);
        };
        for (const string &pkeyStr : _rawPks) {
            unique_ptr<KKVIterator, decltype(deleter)> kkvIter(nullptr, deleter);
            autil::StringView pkeyCS(pkeyStr);
            kkvIter.reset(_kkvReader->Lookup(
                pkeyCS, timestamp, tsc_default, _param.scanResource.queryPool, nullptr));
            if (nullptr == kkvIter) {
                SQL_LOG(WARN, "kkv iterator is nullptr, for pk [%s]", pkeyStr.c_str());
                continue;
            }
            autil::MultiChar pkey = autil::MultiValueCreator::createMultiValueBuffer(
                pkeyStr.c_str(), pkeyStr.size(), _param.scanResource.queryPool);
            MatchDoc curMatchDoc = INVALID_MATCHDOC;
            int matchDocCount = 0;
            CollectorUtil::moveToNextVaildDoc(
                kkvIter.get(), curMatchDoc, _matchDocAllocator, _valueCollector, matchDocCount);
            while (CollectorUtil::isVaildMatchDoc(curMatchDoc)) {
                matchDocs.push_back(curMatchDoc);
                if (nullptr != pkeyRef) {
                    pkeyRef->set(curMatchDoc, pkey);
                }
                CollectorUtil::moveToNextVaildDoc(
                    kkvIter.get(), curMatchDoc, _matchDocAllocator, _valueCollector, matchDocCount);
            }
            kkvIter->Finish();
        }
        return true;
    }
    default:
        SQL_LOG(ERROR, "table [%s] unsupported pkey type", _param.tableName.c_str());
        return false;
    }
#undef NUM_PKEY_CASE_CLAUSE
}

} // namespace sql
} // namespace isearch
