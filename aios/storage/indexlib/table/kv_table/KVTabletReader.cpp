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
#include "indexlib/table/kv_table/KVTabletReader.h"

#include "indexlib/config/TabletSchema.h"
#include "indexlib/index/attribute/AttrHelper.h"
#include "indexlib/index/common/FieldTypeTraits.h"
#include "indexlib/index/kv/KVIndexReader.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/index/kv/config/ValueConfig.h"
#include "indexlib/table/common/SearchUtil.h"
#include "indexlib/table/kv_table/KVCacheReaderImpl.h"
#include "indexlib/util/ProtoJsonizer.h"
namespace indexlibv2::table {

AUTIL_LOG_SETUP(indexlib.table, KVTabletReader);

Status KVTabletReader::DoOpen(const std::shared_ptr<framework::TabletData>& tabletData,
                              const framework::ReadResource& readResource)
{
    auto indexConfigs = _schema->GetIndexConfigs();
    // TODO(xinfei.sxf) indexConfig is possible not kv
    for (auto indexConfig : indexConfigs) {
        const auto& indexType = indexConfig->GetIndexType();
        const auto& indexName = indexConfig->GetIndexName();
        auto indexReader = std::make_shared<KVCacheReaderImpl>(_schema->GetSchemaId());
        auto status = indexReader->Open(indexConfig, tabletData.get());
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "create indexReader IndexType[%s] indexName[%s] failed, status[%s].", indexType.c_str(),
                      indexName.c_str(), status.ToString().c_str());
            return status;
        }
        indexReader->SetSearchCache(readResource.searchCache);
        _indexReaderMap[std::make_pair(indexType, indexName)] = indexReader;
    }
    return Status::OK();
}

Status KVTabletReader::Search(const std::string& jsonQuery, std::string& result) const
{
    std::string indexName;
    try {
        autil::legacy::Any a = autil::legacy::json::ParseJson(jsonQuery);
        autil::legacy::json::JsonMap am = autil::legacy::AnyCast<autil::legacy::json::JsonMap>(a);
        auto iter = am.find("indexName");
        if (iter != am.end()) {
            indexName = autil::legacy::AnyCast<std::string>(iter->second);
        }
    } catch (const std::exception& e) {
        return Status::InvalidArgs("invalid query [%s], exception [%s]", jsonQuery.c_str(), e.what());
    }
    auto tabletSchema = GetSchema();
    if (!tabletSchema) {
        return Status::InternalError("get tablet schema fail.");
    }
    std::shared_ptr<config::IIndexConfig> indexConfig;
    if (indexName.empty()) {
        auto indexConfigs = tabletSchema->GetIndexConfigs("kv");
        if (indexConfigs.size() != 1) {
            std::string errStr = "not support multi kv index yet";
            return Status::InvalidArgs(errStr.c_str());
        }
        indexConfig = indexConfigs[0];
    } else {
        indexConfig = tabletSchema->GetIndexConfig("kv", indexName);
        if (!indexConfig) {
            return Status::InvalidArgs("no indexer with index_name: [%s] exist in schema.", indexName.c_str());
        }
    }
    assert(indexConfig);

    base::PartitionQuery query;
    RETURN_STATUS_DIRECTLY_IF_ERROR(indexlib::util::ProtoJsonizer::FromJson(jsonQuery, &query));
    base::PartitionResponse partitionResponse;
    RETURN_STATUS_DIRECTLY_IF_ERROR(QueryIndex(indexConfig, query, partitionResponse));
    if (indexName.empty()) {
        RETURN_STATUS_DIRECTLY_IF_ERROR(indexlib::util::ProtoJsonizer::ToJson(partitionResponse, &result));
    } else {
        SearchUtil::ConvertResponseToStringFormat(partitionResponse, result);
    }
    return Status::OK();
}

Status KVTabletReader::QueryIndex(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                  const base::PartitionQuery& query, base::PartitionResponse& partitionResponse) const
{
    auto kvConfig = std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(indexConfig);
    if (!kvConfig) {
        return Status::InvalidArgs("failed to cast IIndexConfig to KVIndexConfig");
    }
    auto valueConfig = kvConfig->GetValueConfig();
    assert(valueConfig);
    std::shared_ptr<indexlibv2::index::PackAttributeFormatter> packAttributeFormatter;
    if (valueConfig->IsSimpleValue()) {
        // do nothing
    } else {
        packAttributeFormatter = std::make_shared<indexlibv2::index::PackAttributeFormatter>();
        auto [status, packAttrConfig] = valueConfig->CreatePackAttributeConfig();
        RETURN_IF_STATUS_ERROR(status, "create pack attribute config fail, indexName[%s]",
                               indexConfig->GetIndexName().c_str());
        if (!packAttributeFormatter->Init(packAttrConfig)) {
            return Status::InvalidArgs("pack attribute formatter init failed");
        }
    }

    bool hasPkStrInput = query.pk_size() > 0;
    bool hasPkNumberInput = query.pknumber_size() > 0;
    auto indexName = kvConfig->GetIndexName();
    auto kvReader = GetIndexReader<index::KVIndexReader>("kv", indexName);
    if (!kvReader) {
        return Status::InvalidArgs("failed to get kv reader, indexName[%s]", indexName.c_str());
    }
    auto attrs = ValidateAttrs(kvReader, std::vector<std::string> {query.attrs().begin(), query.attrs().end()});

    if (hasPkStrInput) {
        std::vector<autil::StringView> pkList(query.pk().begin(), query.pk().end());
        RETURN_STATUS_DIRECTLY_IF_ERROR(FillRows(kvReader, attrs, pkList, packAttributeFormatter, partitionResponse));
    } else if (hasPkNumberInput) {
        std::vector<uint64_t> pkList(query.pknumber().begin(), query.pknumber().end());
        RETURN_STATUS_DIRECTLY_IF_ERROR(FillRows(kvReader, attrs, pkList, packAttributeFormatter, partitionResponse));
    } else {
        return Status::InvalidArgs("PKTable PartitionQuery should contain one and only one of pk/pknumber");
    }
    return Status::OK();
}

std::vector<std::string> KVTabletReader::ValidateAttrs(const std::shared_ptr<index::KVIndexReader>& kvReader,
                                                       const std::vector<std::string>& attrs)
{
    std::vector<std::string> kvAttrs;
    auto valueConfig = kvReader->GetKVIndexConfig()->GetValueConfig();
    for (size_t i = 0; i < valueConfig->GetAttributeCount(); i++) {
        kvAttrs.push_back(valueConfig->GetAttributeConfig(i)->GetAttrName());
    }
    return indexlib::index::AttrHelper::ValidateAttrs(kvAttrs, attrs);
}

template <typename PkType>
Status KVTabletReader::QueryAttrWithPk(
    const std::shared_ptr<index::KVIndexReader>& kvReader, const PkType& pk, const std::vector<std::string>& attrs,
    const std::shared_ptr<indexlibv2::index::PackAttributeFormatter>& packAttributeFormatter, base::Row& row)
{
    autil::mem_pool::Pool pool;
    autil::StringView value;
    // 对多个 attr 的查询只取一次 doc Value
    index::KVReadOptions options;
    options.pool = &pool;
    auto ret = future_lite::interface::syncAwait(kvReader->GetAsync(pk, value, options));
    switch (ret) {
    case index::KVResultStatus::NOT_FOUND:
        return Status::NotFound("no record found, [NOT_FOUND]");
    case index::KVResultStatus::DELETED:
        return Status::NotFound("no record found, [DELETED]");
    case index::KVResultStatus::FAIL:
        return Status::NotFound("no record found, [FAIL]");
    default:
        break;
    }
    if (ret != index::KVResultStatus::FOUND) {
        return Status::NotFound("no record found, ret[%d]", (int)ret);
    }
    for (const auto& attr : attrs) {
        FormatKVResult(kvReader, attr, value, packAttributeFormatter, row);
    }
    if constexpr (std::is_integral<PkType>::value) {
        row.set_pk(std::to_string(pk));
    } else {
        row.set_pk(std::string(pk.data(), pk.size()));
    }
    return Status::OK();
}

void KVTabletReader::FormatKVResult(
    const std::shared_ptr<index::KVIndexReader>& kvReader, const std::string& fieldName,
    const autil::StringView& docValue,
    const std::shared_ptr<indexlibv2::index::PackAttributeFormatter>& packAttributeFormatter, base::Row& row)
{
    auto attrValue = row.add_attrvalues();
    if (kvReader->GetValueType() == index::KVVT_TYPED) {
        const auto& valueConfig = kvReader->GetKVIndexConfig()->GetValueConfig();
        const auto& attrConfig = valueConfig->GetAttributeConfig(0);
        FieldType fieldType = attrConfig->GetFieldType();
        GetKvValue(docValue, fieldType, *attrValue);
    } else {
        assert(packAttributeFormatter);
        indexlib::index::AttrHelper::GetAttributeValue(docValue, packAttributeFormatter, fieldName, *attrValue);
    }
}

template <typename T>
Status
KVTabletReader::FillRows(const std::shared_ptr<index::KVIndexReader>& kvReader, const std::vector<std::string>& attrs,
                         const std::vector<T>& pkList,
                         const std::shared_ptr<indexlibv2::index::PackAttributeFormatter>& packAttributeFormatter,
                         base::PartitionResponse& partitionResponse)
{
    for (int i = 0; i < pkList.size(); ++i) {
        base::Row row;
        if constexpr (std::is_integral<T>::value) {
            // Note: If it fails or not found, do not return directly, need process the next pk.
            auto st = QueryAttrWithPk(kvReader, pkList[i], attrs, packAttributeFormatter, row);
            if (!st.is_ok()) {
                continue;
            }
            partitionResponse.add_rows()->Swap(&row);
        } else {
            autil::StringView constStr(pkList[i].data(), pkList[i].size());
            auto st = QueryAttrWithPk(kvReader, constStr, attrs, packAttributeFormatter, row);
            if (!st.is_ok()) {
                continue;
            }
            partitionResponse.add_rows()->Swap(&row);
        }
    }
    indexlib::index::AttrHelper::FillAttrInfo(attrs, partitionResponse);
    return Status::OK();
}

#define FILL_SIMPLE_ATTRIBUTE(ft, strValueType)                                                                        \
    case ft: {                                                                                                         \
        attrValue.set_type(indexlib::index::AttrTypeTraits2<ft>::valueType);                                           \
        typedef indexlib::index::FieldTypeTraits<ft>::AttrItemType type;                                               \
        type* realValue = (type*)(value.data());                                                                       \
        attrValue.set_##strValueType(*realValue);                                                                      \
        break;                                                                                                         \
    }

void KVTabletReader::GetKvValue(const autil::StringView& value, FieldType fieldType,
                                indexlibv2::base::AttrValue& attrValue)
{
    switch (fieldType) {
        FILL_SIMPLE_ATTRIBUTE(ft_int8, int32_value)
        FILL_SIMPLE_ATTRIBUTE(ft_uint8, uint32_value)
        FILL_SIMPLE_ATTRIBUTE(ft_int16, int32_value)
        FILL_SIMPLE_ATTRIBUTE(ft_uint16, uint32_value)
        FILL_SIMPLE_ATTRIBUTE(ft_int32, int32_value)
        FILL_SIMPLE_ATTRIBUTE(ft_uint32, uint32_value)
        FILL_SIMPLE_ATTRIBUTE(ft_int64, int64_value)
        FILL_SIMPLE_ATTRIBUTE(ft_uint64, uint64_value)
        FILL_SIMPLE_ATTRIBUTE(ft_float, float_value)
        FILL_SIMPLE_ATTRIBUTE(ft_double, double_value)
    default:
        // other FieldType should not appear hear
        break;
    }
#undef FILL_SIMPLE_ATTRIBUTE
}

} // namespace indexlibv2::table
