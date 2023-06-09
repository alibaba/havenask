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
#include "ha3/sql/ops/scan/AggregationScan.h"

#include "matchdoc/ValueType.h"
#include "ha3/sql/ops/util/AsyncCallbackCtxBase.h"
#include "ha3/sql/ops/calc/CalcTable.h"
#include "indexlib/base/Status.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/index/aggregation/AggregationIndexConfig.h"
#include "indexlib/index/aggregation/AggregationReader.h"
#include "indexlib/index/aggregation/Aggregator.h"
#include "indexlib/index/aggregation/Constants.h"
#include "indexlib/index/kv/config/ValueConfig.h"
#include "navi/resource/GraphMemoryPoolResource.h"
#include "navi/engine/AsyncPipe.h"
#include "suez/turing/expression/util/TypeTransformer.h"
#include "table/TableUtil.h"

using namespace autil::result;
using namespace matchdoc;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, AggregationScan);

AggregationScan::AggregationScan(const ScanInitParam &param, navi::AsyncPipePtr pipe)
    : ScanBase(param)
    , _asyncPipe(pipe)
{
    _enableTableInfo = false;
}

AggregationScan::~AggregationScan() {}

bool AggregationScan::doInit() {
    auto tabletReader = (_param.scanResource.getTabletReader(_param.tableName));
    if (tabletReader == nullptr) {
        SQL_LOG(ERROR, "find tablet [%s] reader failed", _param.tableName.c_str());
        return false;
    }

    auto schema = tabletReader->GetSchema();

    if (!createMatchDocAllocator(schema)) {
        SQL_LOG(ERROR, "create match doc allocator failed");
        return false;
    }

    _keyCollector = std::make_shared<AggIndexKeyCollector>();
    if (!_keyCollector->init(_param.aggIndexName,
                             schema,
                             _matchDocAllocator)) {
        SQL_LOG(ERROR, "table [%s] KeyCollector init failed", _param.tableName.c_str());
        return false;
    }

    if (_keyCollector->count() != _param.aggKeys.size()) {
        SQL_LOG(ERROR, "index key size [%zu] not equal agg key [%zu]", _keyCollector->count(),
                _param.aggKeys.size());
        return false;
    }

    ValueCollectorPtr valueCollector(new ValueCollector());
    auto indexConfig = schema->GetIndexConfig(indexlibv2::index::AGGREGATION_INDEX_TYPE_STR, _param.aggIndexName);
    if (indexConfig == nullptr) {
        SQL_LOG(ERROR, "get index config failed");
        return false;
    }
    auto aggIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::AggregationIndexConfig>(indexConfig);
    if (aggIndexConfig == nullptr) {
        SQL_LOG(ERROR, "get agg index config failed");
        return false;
    }
    if (!valueCollector->init(aggIndexConfig->GetValueConfig(),
                              _matchDocAllocator,
                              schema->GetTableType())) {
        SQL_LOG(ERROR, "table [%s] ValueCollector init failed!", _param.tableName.c_str());
        return false;
    }
    _valueCollector = valueCollector;

    auto indexReader = tabletReader->GetIndexReader(indexlibv2::index::AGGREGATION_INDEX_TYPE_STR, _param.aggIndexName);
    if (indexReader == nullptr) {
        SQL_LOG(ERROR, "not found index reader for index name [%s]", _param.aggIndexName.c_str());
        return false;
    }
    _aggReader = std::dynamic_pointer_cast<indexlibv2::index::AggregationReader>(indexReader);
    if (_aggReader == nullptr) {
        SQL_LOG(ERROR, "not found aggregation index for index name [%s]", _param.aggIndexName.c_str());
        return false;
    }

    std::vector<autil::StringView> rawKeys;
    for (const auto &key : _param.aggKeys) {
        rawKeys.emplace_back(key);
    }
    _hashKey = _aggReader->CalculateKeyHash(rawKeys);

    CalcInitParam calcInitParam(_param.outputFields,
                                _param.outputFieldsType,
                                _param.conditionJson,
                                _param.outputExprsJson);

    auto &scanResource = _param.scanResource;
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

    if (_asyncPipe) {
        (void)_asyncPipe->setData(ActivationData::inst());
    }

    return true;
}

#define RETURN_IF_ERROR(s)                                              \
    if (!s.is_ok()) {                                                   \
        SQL_LOG(ERROR, "table [%s], index [%s], agg type [%s] failed, error msg: %s", \
                _param.tableName.c_str(), _param.aggIndexName.c_str(), _param.aggType.c_str(), \
                s.get_error().message().c_str());                       \
        return false;                                                   \
    }                                                                   \

bool AggregationScan::createMatchDocAllocator(const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema) {
    auto poolPtr = _param.scanResource.memoryPoolResource->getPool();
    if (poolPtr == nullptr) {
        return false;
    }

    const auto &tableName = schema->GetTableName();
    MountInfoPtr mountInfo(new MountInfo());
    uint32_t beginMountId = 0;

    auto indexConfig = schema->GetIndexConfig(indexlibv2::index::AGGREGATION_INDEX_TYPE_STR, _param.aggIndexName);
    if (indexConfig == nullptr) {
        SQL_LOG(ERROR, "get index config failed");
        return false;
    }
    auto aggIndexConfig = std::dynamic_pointer_cast<indexlibv2::config::AggregationIndexConfig>(indexConfig);
    const auto &valueConfig = aggIndexConfig->GetValueConfig();
    assert(valueConfig);
    auto [status, packConf] = valueConfig->CreatePackAttributeConfig();
    THROW_IF_STATUS_ERROR(status);
    CollectorUtil::insertPackAttributeMountInfo(mountInfo, packConf, tableName, beginMountId++);
    _matchDocAllocator = std::make_shared<matchdoc::MatchDocAllocator>(
            poolPtr, false, mountInfo);
    return true;
}

bool AggregationScan::doBatchScan(table::TablePtr &table, bool &eof) {
    eof = true;
    autil::Result<table::TablePtr> s;
    if (_param.aggType == "count") {
        s = countAggFunc();
        RETURN_IF_ERROR(s);
    } else if (_param.aggType == "sum") {
        s = sumAggFunc();
        RETURN_IF_ERROR(s);
    } else if (_param.aggType == "lookup") {
        s = lookupFunc();
        RETURN_IF_ERROR(s);
    } else {
        SQL_LOG(ERROR, "unsupport agg type: %s", _param.aggType.c_str());
        return false;
    }

    table = s.get();

    SQL_LOG(TRACE3, "before calc table: %s", table::TableUtil::toString(table, 50).c_str());
    if (!_calcTable->calcTable(table)) {
        SQL_LOG(ERROR, "calc table failed");
        return false;
    }
    SQL_LOG(TRACE3, "after calc table: %s", table::TableUtil::toString(table, 50).c_str());
    

    return true;
}

autil::Result<table::TablePtr> AggregationScan::countAggFunc() {
    auto pool = _param.scanResource.memoryPoolResource->getPool();
    indexlibv2::index::Aggregator agg(_aggReader.get(), pool.get());
    agg.Key(_hashKey);
    autil::Result<uint64_t> r;
    if (_param.aggDistinct) {
        r = agg.DistinctCount(_param.aggValueField);
    } else {
        r = agg.Count();
    }
    if (!r.is_ok()) {
        return {r.steal_error()};
    }
    return createTable(r.get(), _param.outputFields[0], _param.outputFieldsType[0], pool);
}

autil::Result<table::TablePtr> AggregationScan::sumAggFunc() {
    auto pool = _param.scanResource.memoryPoolResource->getPool();
    indexlibv2::index::Aggregator agg(_aggReader.get(), pool.get());
    agg.Key(_hashKey);
    autil::Result<double> r;
    if (!_param.aggDistinct) {
        r = agg.Sum(_param.aggValueField);
    } else {
        // TODO: @suhang, need fix
        r = agg.DistinctSum(_param.aggValueField, _param.aggValueField);
    }
    if (!r.is_ok()) {
        return {r.steal_error()};
    }
    return createTable(r.get(), _param.outputFields[0], _param.outputFieldsType[0], pool);
}

autil::Result<table::TablePtr> AggregationScan::lookupFunc() {
    if (_matchDocAllocator == nullptr) {
        return RuntimeError::make("prepare matchdoc allocator failed");
    }

    auto creator = std::make_unique<indexlibv2::index::ValueIteratorCreator>(_aggReader.get());
    if (!_param.aggRangeMap.empty()) {
        const auto &iter = _param.aggRangeMap.begin();
        creator->FilterBy(iter->first, iter->second.first, iter->second.second);
    }
    auto pool = _matchDocAllocator->getPoolPtr().get();
    auto s = creator->CreateIterator(_hashKey, pool);

    if (!s.IsOK()) {
        return RuntimeError::make("create iter failed, error: %s", s.ToString().c_str());
    }

    auto aggIter = std::move(s.steal_value());
    
    std::vector<matchdoc::MatchDoc> matchdocs;
    while (aggIter->HasNext()) {
        autil::StringView value;
        auto s = aggIter->Next(pool, value);
        if (s.IsEof()) {
            break;
        }
        if (!s.IsOK()) {
            return RuntimeError::make("get value failed, error: %s", s.ToString().c_str());
        }
        if (!appendMatchdoc(value, matchdocs)) {
            return RuntimeError::make("append value failed");
        }
    }

    return {ScanBase::createTable(matchdocs, _matchDocAllocator, true)};
}

bool AggregationScan::appendMatchdoc(const autil::StringView &view, std::vector<matchdoc::MatchDoc> &matchdocs) {
    MatchDoc doc = _matchDocAllocator->allocate();

    for (size_t i = 0; i < _keyCollector->count(); i++) {
        auto ref = _keyCollector->getRef(i);
        auto fieldType = _keyCollector->getFieldType(i);
        auto bt = suez::turing::TypeTransformer::transform(fieldType);

        switch(bt) {
#define CASE(bt)                                \
            case bt: {                          \
                typedef MatchDocBuiltinType2CppType<bt, false>::CppType T; \
                auto typedRef = dynamic_cast<Reference<T> *>(ref);      \
                T typedKey;                                             \
                if (!autil::StringUtil::fromString<T>(_param.aggKeys[i], typedKey)) { \
                    SQL_LOG(ERROR, "convert key [%zu] form [%s] failed", i, _param.aggKeys[i].c_str()); \
                    return false;                                       \
                }                                                       \
                typedRef->set(doc, typedKey);                           \
                break;                                                  \
            }
            NUMBER_BUILTIN_TYPE_MACRO_HELPER(CASE);
#undef CASE
        case matchdoc::BuiltinType::bt_string: {
            auto typedRef = dynamic_cast<Reference<autil::MultiChar> *>(ref);
            autil::StringView typedKey;
            if (!autil::StringUtil::fromString(_param.aggKeys[i], typedKey)) {
                SQL_LOG(ERROR, "convert key [%zu] form [%s] failed", i, _param.aggKeys[i].c_str());
                return false;
            }
            typedRef->set(doc, typedKey);
            break;
        }
        default: {
            SQL_LOG(ERROR, "unsupported type %d:", bt);
            return false;
        }
        }
    }

    _valueCollector->collectFields(doc, view);
    matchdocs.push_back(doc);
    return true;
}

}
}
