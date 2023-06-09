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
#include "suez/service/KVTableSearcher.h"

#include "autil/StringTokenizer.h"
#include "autil/TimeUtility.h"
#include "autil/TimeoutTerminator.h"
#include "autil/legacy/jsonizable.h"
#include "autil/result/Errors.h"
#include "indexlib/base/FieldType.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/framework/ITabletReader.h"
#include "indexlib/index/attribute/AttrHelper.h"
#include "indexlib/index/common/FieldTypeTraits.h"
#include "indexlib/index/kv/Common.h"
#include "indexlib/index/kv/KVIndexReader.h"
#include "indexlib/index/kv/KVReadOptions.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/indexlib.h"
#include "indexlib/table/kv_table/KVTabletSessionReader.h"
#include "table/Table.h"
#include "table/TableUtil.h"

using namespace std;
using namespace autil::result;
using namespace indexlibv2::config;
using namespace indexlibv2::table;
using namespace kmonitor;

namespace suez {

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, KVTableSearcher);

class KVTableSearcherMetrics : public kmonitor::MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_QPS_MUTABLE_METRIC(_qps, "KVTableSearcher.qps");
        REGISTER_GAUGE_MUTABLE_METRIC(_blockCacheHitCount, "KVTableSearcher.blockCacheHitCount");
        REGISTER_GAUGE_MUTABLE_METRIC(_blockCacheMissCount, "KVTableSearcher.blockCacheMissCount");
        REGISTER_GAUGE_MUTABLE_METRIC(_searchCacheHitCount, "KVTableSearcher.searchCacheHitCount");
        REGISTER_GAUGE_MUTABLE_METRIC(_searchCacheMissCount, "KVTableSearcher.searchCacheMissCount");
        REGISTER_GAUGE_MUTABLE_METRIC(_docsCount, "KVTableSearcher.docsCount");
        REGISTER_GAUGE_MUTABLE_METRIC(_failedDocsCount, "KVTableSearcher.failedDocsCount");
        REGISTER_GAUGE_MUTABLE_METRIC(_notFoundDocsCount, "KVTableSearcher.notFoundDocsCount");

        REGISTER_LATENCY_MUTABLE_METRIC(_convertTime, "KVTableSearcher.convertTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_lookupTime, "KVTableSearcher.lookupTime");

        return true;
    }

    void report(const kmonitor::MetricsTags *tags, const KVTableSearcherMetricsCollector *kvMetrics) {
        REPORT_MUTABLE_QPS(_qps);
        REPORT_MUTABLE_METRIC(_blockCacheHitCount, kvMetrics->indexCollector.GetBlockCacheHitCount());
        REPORT_MUTABLE_METRIC(_blockCacheMissCount, kvMetrics->indexCollector.GetBlockCacheMissCount());
        REPORT_MUTABLE_METRIC(_searchCacheHitCount, kvMetrics->indexCollector.GetSearchCacheHitCount());
        REPORT_MUTABLE_METRIC(_searchCacheMissCount, kvMetrics->indexCollector.GetSearchCacheMissCount());
        REPORT_MUTABLE_METRIC(_docsCount, kvMetrics->docsCount);
        REPORT_MUTABLE_METRIC(_failedDocsCount, kvMetrics->failedDocsCount);
        REPORT_MUTABLE_METRIC(_notFoundDocsCount, kvMetrics->notFoundDocsCount);

        REPORT_MUTABLE_METRIC(_convertTime, kvMetrics->convertTime / 1000.0f);
        REPORT_MUTABLE_METRIC(_lookupTime, kvMetrics->lookupTime / 1000.0f);
    }

private:
    MutableMetric *_qps = nullptr;
    MutableMetric *_blockCacheHitCount = nullptr;
    MutableMetric *_blockCacheMissCount = nullptr;
    MutableMetric *_searchCacheHitCount = nullptr;
    MutableMetric *_searchCacheMissCount = nullptr;
    MutableMetric *_docsCount = nullptr;
    MutableMetric *_failedDocsCount = nullptr;
    MutableMetric *_notFoundDocsCount = nullptr;
    MutableMetric *_convertTime = nullptr;
    MutableMetric *_lookupTime = nullptr;
};

KVTableSearcher::KVTableSearcher(const MultiTableReader *multiTableReader) : _multiTableReader(multiTableReader) {}

KVTableSearcher::~KVTableSearcher() {}

autil::Result<> KVTableSearcher::init(const std::string &tableName, const std::string &indexName) {
    _tableName = tableName;
    _indexName = indexName;
    for (auto [partId, singleReader] : _multiTableReader->getTableReaders(_tableName)) {
        if (!singleReader) {
            continue;
        }
        auto tablet = singleReader->getTablet();
        if (!tablet) {
            return autil::result::RuntimeError::make("tablet is nullptr");
        }
        auto tabletReader = tablet->GetTabletReader();
        if (!tabletReader) {
            return autil::result::RuntimeError::make("tablet reader is nullptr");
        }
        _tabletReaderHolder[partId.index] = tabletReader;
    }
    if (_tabletReaderHolder.empty()) {
        return autil::result::RuntimeError::make("multi table reader has no tablet reader");
    }
    if (_indexName.empty()) {
        // 如果indexName = “”，那么从indexConfigs中获取indexName
        auto tabletReader = _tabletReaderHolder[_tabletReaderHolder.begin()->first];
        auto schema = tabletReader->GetSchema();
        if (!schema) {
            return autil::result::RuntimeError::make("get schema failed!");
        }
        auto indexConfigs = schema->GetIndexConfigs();
        if (!indexConfigs.size()) {
            return autil::result::RuntimeError::make("indexconfig is empty!");
        }
        _indexName = indexConfigs[0]->GetIndexName();
    }
    return {};
}

autil::Result<LookupResult> KVTableSearcher::lookup(const InputHashKeys &inputHashKeys, LookupOptions *options) {
    auto beginLookupTime = autil::TimeUtility::monotonicTimeUs();
    LookupResult lookupResult;
    std::vector<std::vector<autil::StringView>> values(inputHashKeys.size());
    auto kvOptions = createKvOptions(options);
    for (auto i = 0; i < inputHashKeys.size(); i++) {
        if (_tabletReaderHolder.find(inputHashKeys[i].first) == _tabletReaderHolder.end()) {
            continue;
        }
        auto tabletReader = _tabletReaderHolder[inputHashKeys[i].first];
        if (!tabletReader) {
            return autil::result::RuntimeError::make("tablet reader is nullptr");
        }
        auto indexReader = tabletReader->GetIndexReader(indexlibv2::index::KV_INDEX_TYPE_STR, _indexName);
        auto kvReader = dynamic_pointer_cast<indexlibv2::index::KVIndexReader>(indexReader);
        if (kvReader == nullptr) {
            return RuntimeError::make("cast kv reader failed");
        }
        for (auto &key : inputHashKeys[i].second) {
            autil::StringView value;
            auto res = kvReader->Get(key, value, kvOptions);
            if (res == indexlibv2::index::KVResultStatus::FOUND) {
                lookupResult.foundValues[key] = std::move(value);
            } else if (res == indexlibv2::index::KVResultStatus::NOT_FOUND) {
                lookupResult.notFoundCount++;
                _metricsCollector.notFoundDocsCount++;
            } else {
                AUTIL_LOG(ERROR, "lookup failed, KVResultStatus: [%d]", (int)res);
                lookupResult.failedCount++;
                _metricsCollector.failedDocsCount++;
            }
        }
    }
    _metricsCollector.lookupTime = autil::TimeUtility::monotonicTimeUs() - beginLookupTime;
    return lookupResult;
}

autil::Result<std::vector<uint64_t>> KVTableSearcher::genHashKeys(size_t partId, const std::vector<std::string> &keys) {
    auto tabletReader = _tabletReaderHolder[partId];

    if (tabletReader == nullptr) {
        return autil::result::RuntimeError::make("not found tablet reader, partId %zu", partId);
    }

    auto indexReader = tabletReader->GetIndexReader(indexlibv2::index::KV_INDEX_TYPE_STR, _indexName);
    if (indexReader == nullptr) {
        return autil::result::RuntimeError::make("not found kv index %s, partId %zu", _indexName.c_str(), partId);
    }
    auto kvReader = dynamic_pointer_cast<indexlibv2::index::KVIndexReader>(indexReader);
    if (kvReader == nullptr) {
        return autil::result::RuntimeError::make(
            "cast kv index failed, index name %s, partId %zu", _indexName.c_str(), partId);
    }
    std::vector<uint64_t> hashKeys;
    for (auto key : keys) {
        dictkey_t hashKey;
        kvReader->GetHashKeyWithType(kvReader->GetHasherType(), autil::StringView(key), hashKey);
        hashKeys.emplace_back(hashKey);
    }
    return hashKeys;
}

indexlibv2::index::KVReadOptions KVTableSearcher::createKvOptions(LookupOptions *lookupOpts) {
    indexlibv2::index::KVReadOptions options;
    options.timestamp = autil::TimeUtility::currentTime();
    options.timeoutTerminator = std::make_shared<autil::TimeoutTerminator>(lookupOpts->timeout);
    options.pool = lookupOpts->pool;
    options.metricsCollector = &_metricsCollector.indexCollector;
    return options;
}

template <typename T>
bool convertSimpleColumn(table::Table *table,
                         const std::string &columnName,
                         const std::vector<autil::StringView> &values) {
    auto column = table->template declareColumn<T>(columnName);
    if (column == nullptr) {
        return false;
    }
    auto columnData = column->template getColumnData<T>();
    for (size_t i = 0; i < values.size(); i++) {
        columnData->set(i, (*(T *)values[i].data()));
    }
    return true;
}

template <typename T>
bool convertPackedColumn(table::Table *table,
                         const std::string &columnName,
                         indexlibv2::index::AttributeReference *attrFetcher,
                         const std::vector<autil::StringView> &values,
                         bool isMulti) {
    if (isMulti) {
        typedef autil::MultiValueType<T> MultiValueType;
        auto column = table->template declareColumn<MultiValueType>(columnName);
        if (column == nullptr) {
            return false;
        }
        auto columnData = column->template getColumnData<MultiValueType>();

        for (size_t i = 0; i < values.size(); i++) {
            MultiValueType multiValue;
            if (!indexlib::index::AttrHelper::FetchTypeValue<MultiValueType>(
                    attrFetcher, values[i].data(), multiValue)) {
                return false;
            }
            columnData->set(i, multiValue);
        }
    } else {
        auto column = table->template declareColumn<T>(columnName);
        if (column == nullptr) {
            return false;
        }
        auto columnData = column->template getColumnData<T>();

        for (size_t i = 0; i < values.size(); i++) {
            T singleValue;
            if (!indexlib::index::AttrHelper::FetchTypeValue<T>(attrFetcher, values[i].data(), singleValue)) {
                return false;
            }
            columnData->set(i, singleValue);
        }
    }
    return true;
}

autil::Result<std::string>
KVTableSearcher::convertResult(size_t partId, const std::vector<autil::StringView> &values, LookupOptions *options) {
    auto beginConvertTime = autil::TimeUtility::monotonicTimeUs();
    auto tabletReader = _tabletReaderHolder[partId];
    if (tabletReader == nullptr) {
        return RuntimeError::make("get tablet reader failed, %zu", partId);
    }

    auto indexReader = tabletReader->GetIndexReader(indexlibv2::index::KV_INDEX_TYPE_STR, _indexName);
    if (indexReader == nullptr) {
        return RuntimeError::make("get index reader failed");
    }
    auto reader = dynamic_pointer_cast<indexlibv2::index::KVIndexReader>(indexReader);
    if (reader == nullptr) {
        return RuntimeError::make("not kv reader");
    }

    auto valueType = reader->GetValueType();
    auto valueConfig = reader->GetKVIndexConfig()->GetValueConfig();
    std::shared_ptr<indexlibv2::index::PackAttributeFormatter> packAttributeFormatter;
    if (valueType != indexlibv2::index::KVVT_TYPED) {
        packAttributeFormatter = std::make_shared<indexlibv2::index::PackAttributeFormatter>();
        auto [status, packAttrConfig] = valueConfig->CreatePackAttributeConfig();
        if (!status.IsOK()) {
            return RuntimeError::make(status.ToString());
        }
        if (!packAttributeFormatter->Init(packAttrConfig)) {
            return RuntimeError::make("pack attribute formatter init failed");
        }
    }

    auto pool = autil::mem_pool::PoolPtr(new autil::mem_pool::Pool(1024));
    auto table = std::make_shared<table::Table>(pool);
    table->batchAllocateRow(values.size());

    for (const auto &attr : options->attrs) {
        if (indexlibv2::index::KVVT_TYPED == valueType) {
            const auto &attrConfig = valueConfig->GetAttributeConfig(0);
            if (!attrConfig) {
                return RuntimeError::make("field name: %s not found in index", attr.c_str());
            }

            FieldType fieldType = attrConfig->GetFieldType();
#define CASE(ft)                                                                                                       \
    case (ft): {                                                                                                       \
        typedef indexlib::index::FieldTypeTraits<ft>::AttrItemType InnerType;                                          \
        if (!convertSimpleColumn<InnerType>(table.get(), attr, values)) {                                              \
            return RuntimeError::make("convert simple column [%s] failed", attr.c_str());                              \
        }                                                                                                              \
    } break;

            switch (fieldType) {
                BASIC_NUMBER_FIELD_MACRO_HELPER(CASE);
            default:
                return RuntimeError::make("not support simple value type: %d", fieldType);
                break;
            }
#undef CASE
        } else {
            auto attrConfig = packAttributeFormatter->GetAttributeConfig(attr);
            auto attrFetcher = packAttributeFormatter->GetAttributeReference(attr);
            if (!attrConfig || !attrFetcher) {
                return RuntimeError::make("field name: %s not found in index", attr.c_str());
            }
            auto isMultiValue = attrConfig->IsMultiValue();
            FieldType fieldType = attrConfig->GetFieldType();

#define PACKED_ATTR_CASE(ft)                                                                                           \
    case (ft): {                                                                                                       \
        typedef indexlib::index::FieldTypeTraits<ft>::AttrItemType InnerType;                                          \
        if (!convertPackedColumn<InnerType>(table.get(), attr, attrFetcher, values, isMultiValue)) {                   \
            return RuntimeError::make("convert packed column [%s] failed", attr.c_str());                              \
        }                                                                                                              \
    } break;

            switch (fieldType) {
                BASIC_NUMBER_FIELD_MACRO_HELPER(PACKED_ATTR_CASE);
                PACKED_ATTR_CASE(FieldType::ft_string);
            default:
                return RuntimeError::make("not support packed value type: %d", fieldType);
                break;
            }
        }
    }
#undef PACKED_ATTR_CASE

    string data;
    AUTIL_LOG(DEBUG, "lookup result is %s", table::TableUtil::toString(table, 20).c_str());

    table->serializeToString(data, options->pool);
    _metricsCollector.convertTime = autil::TimeUtility::monotonicTimeUs() - beginConvertTime;
    return data;
}

void KVTableSearcher::reportMetrics(kmonitor::MetricsReporter *searchMetricsReporter) const {
    searchMetricsReporter->report<KVTableSearcherMetrics>(nullptr, &_metricsCollector);
}

autil::result::Result<std::vector<std::string>> KVTableSearcher::query(const InputKeys &inputKeys,
                                                                       LookupOptions *options) {
    std::vector<std::string> results;
    for (auto [partId, keys] : inputKeys) {
        auto query = constructJsonQuery(keys, options->attrs);
        auto singleReader = _multiTableReader->getTableReaderByIdx(_tableName, partId);
        if (!singleReader) {
            continue;
        }
        auto tablet = singleReader->getTablet();
        if (!tablet) {
            return autil::result::RuntimeError::make("tablet is nullptr");
        }
        auto tabletReader = tablet->GetTabletReader();
        if (!tabletReader) {
            return autil::result::RuntimeError::make("tablet reader is nullptr");
        }
        std::string result;
        auto status = tabletReader->Search(query, result);
        if (!status.IsOK()) {
            return autil::result::RuntimeError::make(status.ToString());
        }
        results.emplace_back(result);
    }
    return results;
}

std::string KVTableSearcher::constructJsonQuery(const std::vector<std::string> &keys,
                                                const std::vector<std::string> &attrs) {
    autil::legacy::json::JsonMap jsonMap;
    autil::legacy::json::JsonArray emptyArray;
    jsonMap["index_name"] = autil::legacy::ToJson(_indexName);
    jsonMap["pk_list"] = autil::legacy::ToJson(keys);
    jsonMap["attributes"] = autil::legacy::ToJson(attrs);
    return autil::legacy::ToJsonString(jsonMap);
}

} // namespace suez
