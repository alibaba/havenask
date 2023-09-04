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
#include "sql/ops/scan/KKVScanR.h"

#include <algorithm>
#include <cstddef>
#include <engine/NaviConfigContext.h>
#include <memory>
#include <stdint.h>
#include <unordered_set>
#include <utility>

#include "autil/CommonMacros.h"
#include "autil/MultiValueCreator.h"
#include "autil/MultiValueType.h"
#include "autil/Span.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/Pool.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "indexlib/config/FileCompressSchema.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/index_schema.h"
#include "indexlib/config/kkv_index_config.h"
#include "indexlib/config/legacy_schema_adapter.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/index/attribute/Constant.h"
#include "indexlib/index/kkv/kkv_define.h"
#include "indexlib/index/kkv/kkv_iterator.h"
#include "indexlib/index/kkv/kkv_reader.h"
#include "indexlib/index/kv/Types.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/table/BuiltinDefine.h"
#include "indexlib/util/ErrorLogCollector.h"
#include "indexlib/util/PoolUtil.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/Reference.h"
#include "matchdoc/ValueType.h"
#include "matchdoc/VectorDocStorage.h"
#include "navi/builder/ResourceDefBuilder.h"
#include "navi/proto/KernelDef.pb.h"
#include "sql/common/Log.h"
#include "sql/framework/PushDownOp.h"
#include "sql/ops/calc/CalcInitParamR.h"
#include "sql/ops/condition/Condition.h"
#include "sql/ops/condition/ConditionParser.h"
#include "sql/ops/condition/ConditionVisitor.h"
#include "sql/ops/scan/Collector.h"
#include "sql/ops/scan/PrimaryKeyScanConditionVisitor.h"
#include "sql/ops/scan/ScanBase.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/navi/QueryMemPoolR.h"
#include "table/Table.h"

namespace navi {
class ResourceInitContext;
} // namespace navi

using namespace std;
using namespace autil;
using namespace table;
using namespace matchdoc;
using namespace suez::turing;
using namespace indexlib;
using namespace indexlib::index;
using namespace indexlib::config;
using namespace isearch::common;

namespace sql {

const std::string KKVScanR::RESOURCE_ID = "kkv_scan_r";

KKVScanR::KKVScanR()
    : _canReuseAllocator(false) {}

KKVScanR::~KKVScanR() {}

void KKVScanR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_KERNEL);
}

bool KKVScanR::config(navi::ResourceConfigContext &ctx) {
    NAVI_JSONIZE(ctx, "kkv_require_pk", _requirePk, _requirePk);
    return true;
}

navi::ErrorCode KKVScanR::init(navi::ResourceInitContext &ctx) {
    if (!ScanBase::doInit()) {
        return navi::EC_ABORT;
    }
    _canReuseAllocator = _scanOnce;
    if (!prepareKKVReader()) {
        SQL_LOG(ERROR, "prepare kkv reader failed");
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
    return navi::EC_NONE;
}

bool KKVScanR::doBatchScan(TablePtr &table, bool &eof) {
    _scanInitParamR->incTotalScanCount(_rawPks.size());

    autil::ScopedTime2 seekTimer;
    std::vector<matchdoc::MatchDoc> matchDocs;
    if (!fillMatchDocs(matchDocs)) {
        SQL_LOG(ERROR, "fill kkv failed");
        return false;
    }
    _scanInitParamR->incSeekTime(seekTimer.done_us());

    autil::ScopedTime2 outputTimer;
    table = createTable(
        matchDocs, _attributeExpressionCreatorR->_matchDocAllocator, _canReuseAllocator);
    _scanInitParamR->incOutputTime(outputTimer.done_us());

    autil::ScopedTime2 evaluteTimer;
    if (!_calcTableR->calcTable(table)) {
        SQL_LOG(ERROR, "calc table failed");
        return false;
    }
    _seekCount += table->getRowCount();
    _scanInitParamR->incEvaluateTime(evaluteTimer.done_us());

    outputTimer = {};
    if (_limit < _seekCount) {
        table->clearBackRows(min(_seekCount - _limit, (uint32_t)table->getRowCount()));
    }
    _scanInitParamR->incOutputTime(outputTimer.done_us());

    eof = true;
    return true;
}

bool KKVScanR::doUpdateScanQuery(const StreamQueryPtr &inputQuery) {
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

bool KKVScanR::parseQuery() {
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
                errorMsg = "condition json: " + conditionJson;
            }
            SQL_LOG(ERROR,
                    "kkv table name [%s] need key query, %s",
                    _scanInitParamR->tableName.c_str(),
                    errorMsg.c_str());
        }
        return !_requirePk;
    }
    filterPks(visitor.getRawKeyVec());
    return true;
}

void KKVScanR::filterPks(const std::vector<std::string> &pks) {
    size_t size = pks.size();
    _rawPks.reserve(size / _scanInitParamR->parallelNum + 1);
    for (size_t i = _scanInitParamR->parallelIndex; i < size; i += _scanInitParamR->parallelNum) {
        _rawPks.emplace_back(pks[i]);
    }
}

bool KKVScanR::prepareKKVReader() {
    const string &tableName = _scanInitParamR->tableName;
    const auto &indexPartitionReaderWrapper
        = _attributeExpressionCreatorR->_indexPartitionReaderWrapper;
    auto schema = indexPartitionReaderWrapper->getSchema();
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
        SQL_LOG(
            ERROR, "schema[%s] cast to LegacySchemaAdapter fail", schema->GetTableName().c_str());
        return false;
    }

    _kkvReader = indexPartitionReaderWrapper->getKKVReader();
    if (!_kkvReader) {
        SQL_LOG(ERROR, "table [%s] get kkv reader failed", tableName.c_str());
        return false;
    }
    return true;
}

bool KKVScanR::prepareFields() {
    const auto &matchDocAllocator = _attributeExpressionCreatorR->_matchDocAllocator;
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
        // indexlibv2 does not support kkv yet
        SQL_LOG(
            ERROR, "schema[%s] cast to LegacySchemaAdapter fail", _schema->GetTableName().c_str());
        return false;
    }

    _pkeyCollector = pkeyCollector;
    _valueCollector = valueCollector;
    return true;
}

bool KKVScanR::fillMatchDocs(std::vector<matchdoc::MatchDoc> &matchDocs) {
    if (!_pkeyCollector || !_valueCollector || !_kkvReader) {
        return false;
    }
    matchDocs.reserve(_rawPks.size());
    int64_t timestamp = TimeUtility::currentTime();
    const auto &matchDocAllocator = _attributeExpressionCreatorR->_matchDocAllocator;

#define NUM_PKEY_CASE_CLAUSE(key_t)                                                                \
    case key_t: {                                                                                  \
        if (unlikely(!_kkvReader->GetKKVIndexConfig()->UseNumberHash())) {                         \
            SQL_LOG(                                                                               \
                ERROR, "table [%s] pkey type is not-number", _scanInitParamR->tableName.c_str());  \
            return false;                                                                          \
        }                                                                                          \
        typedef MatchDocBuiltinType2CppType<key_t, false>::CppType T;                              \
        auto pkeyRef = dynamic_cast<Reference<T> *>(_pkeyCollector->getPkeyRef());                 \
        auto deleter = [this](indexlib::index::KKVIterator *p) {                                   \
            IE_POOL_COMPATIBLE_DELETE_CLASS(_queryMemPoolR->getPool().get(), p);                   \
        };                                                                                         \
        for (const string &pkeyStr : _rawPks) {                                                    \
            unique_ptr<KKVIterator, decltype(deleter)> kkvIter(nullptr, deleter);                  \
            T pkey;                                                                                \
            if (!autil::StringUtil::fromString(pkeyStr, pkey)) {                                   \
                return false;                                                                      \
            }                                                                                      \
            kkvIter.reset(_kkvReader->Lookup((PKeyType)pkey,                                       \
                                             timestamp,                                            \
                                             tsc_default,                                          \
                                             _queryMemPoolR->getPool().get(),                      \
                                             nullptr));                                            \
            if (nullptr == kkvIter) {                                                              \
                SQL_LOG(WARN, "kkv iterator is nullptr, for pk [%s]", pkeyStr.c_str());            \
                continue;                                                                          \
            }                                                                                      \
            MatchDoc curMatchDoc = INVALID_MATCHDOC;                                               \
            int matchDocCount = 0;                                                                 \
            CollectorUtil::moveToNextVaildDoc(                                                     \
                kkvIter.get(), curMatchDoc, matchDocAllocator, _valueCollector, matchDocCount);    \
            while (CollectorUtil::isVaildMatchDoc(curMatchDoc)) {                                  \
                matchDocs.push_back(curMatchDoc);                                                  \
                if (nullptr != pkeyRef) {                                                          \
                    pkeyRef->set(curMatchDoc, pkey);                                               \
                }                                                                                  \
                CollectorUtil::moveToNextVaildDoc(kkvIter.get(),                                   \
                                                  curMatchDoc,                                     \
                                                  matchDocAllocator,                               \
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
            IE_POOL_COMPATIBLE_DELETE_CLASS(_queryMemPoolR->getPool().get(), p);
        };
        for (const string &pkeyStr : _rawPks) {
            unique_ptr<KKVIterator, decltype(deleter)> kkvIter(nullptr, deleter);
            autil::StringView pkeyCS(pkeyStr);
            kkvIter.reset(_kkvReader->Lookup(
                pkeyCS, timestamp, tsc_default, _queryMemPoolR->getPool().get(), nullptr));
            if (nullptr == kkvIter) {
                SQL_LOG(WARN, "kkv iterator is nullptr, for pk [%s]", pkeyStr.c_str());
                continue;
            }
            autil::MultiChar pkey = autil::MultiValueCreator::createMultiValueBuffer(
                pkeyStr.c_str(), pkeyStr.size(), _queryMemPoolR->getPool().get());
            MatchDoc curMatchDoc = INVALID_MATCHDOC;
            int matchDocCount = 0;
            CollectorUtil::moveToNextVaildDoc(
                kkvIter.get(), curMatchDoc, matchDocAllocator, _valueCollector, matchDocCount);
            while (CollectorUtil::isVaildMatchDoc(curMatchDoc)) {
                matchDocs.push_back(curMatchDoc);
                if (nullptr != pkeyRef) {
                    pkeyRef->set(curMatchDoc, pkey);
                }
                CollectorUtil::moveToNextVaildDoc(
                    kkvIter.get(), curMatchDoc, matchDocAllocator, _valueCollector, matchDocCount);
            }
            kkvIter->Finish();
        }
        return true;
    }
    default:
        SQL_LOG(ERROR, "table [%s] unsupported pkey type", _scanInitParamR->tableName.c_str());
        return false;
    }
#undef NUM_PKEY_CASE_CLAUSE
}

REGISTER_RESOURCE(KKVScanR);

} // namespace sql
