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
#include "indexlib/table/kkv_table/KKVTabletReader.h"

#include "indexlib/config/TabletSchema.h"
#include "indexlib/index/attribute/AttrHelper.h"
#include "indexlib/index/common/FieldTypeTraits.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "indexlib/table/kkv_table/KKVReader.h"
#include "indexlib/table/kkv_table/KKVReaderFactory.h"
#include "indexlib/util/ProtoJsonizer.h"

namespace indexlibv2::table {

AUTIL_LOG_SETUP(indexlib.table, KKVTabletReader);

Status KKVTabletReader::Open(const std::shared_ptr<framework::TabletData>& tabletData,
                             const framework::ReadResource& readResource)
{
    auto indexConfigs = _schema->GetIndexConfigs();
    for (auto indexConfig : indexConfigs) {
        const auto& indexType = indexConfig->GetIndexType();
        const auto& indexName = indexConfig->GetIndexName();
        if (index::KKV_RAW_KEY_INDEX_NAME == indexName) {
            continue;
        }
        auto kkvIndexConfig = std::dynamic_pointer_cast<config::KKVIndexConfig>(indexConfig);
        assert(kkvIndexConfig);
        auto indexReader = KKVReaderFactory::Create(readResource, kkvIndexConfig, _schema->GetSchemaId());
        if (!indexReader) {
            AUTIL_LOG(ERROR, "create indexReader IndexType[%s] indexName[%s] failed.", indexType.c_str(),
                      indexName.c_str());
            return Status::ConfigError("create indexReader failed");
        }
        auto status = indexReader->Open(kkvIndexConfig, tabletData.get());
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "open indexReader IndexType[%s] indexName[%s] failed, status[%s].", indexType.c_str(),
                      indexName.c_str(), status.ToString().c_str());
            return status;
        }
        _indexReaderMap[std::make_pair(indexType, indexName)] = indexReader;
    }
    return Status::OK();
}

Status KKVTabletReader::Search(const std::string& jsonQuery, std::string& result) const
{
    auto tabletSchema = GetSchema();
    if (!tabletSchema) {
        return Status::InternalError("get tablet schema fail.");
    }
    std::shared_ptr<config::IIndexConfig> indexConfig;
    auto indexConfigs = tabletSchema->GetIndexConfigs("kkv");
    if (indexConfigs.size() != 1) {
        std::string errStr = "not support multi kkv index yet";
        return Status::InvalidArgs(errStr.c_str());
    }
    indexConfig = indexConfigs[0];
    assert(indexConfig);

    base::PartitionQuery query;
    RETURN_STATUS_DIRECTLY_IF_ERROR(indexlib::util::ProtoJsonizer::FromJson(jsonQuery, &query));
    base::PartitionResponse partitionResponse;
    RETURN_STATUS_DIRECTLY_IF_ERROR(QueryIndex(indexConfig, query, partitionResponse));
    RETURN_STATUS_DIRECTLY_IF_ERROR(indexlib::util::ProtoJsonizer::ToJson(partitionResponse, &result));
    return Status::OK();
}

Status KKVTabletReader::QueryIndex(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                   const base::PartitionQuery& query, base::PartitionResponse& partitionResponse) const
{
    if (query.pk_size() != 1) {
        return Status::InvalidArgs("PKTable PartitionQuery should contain one and only one of pk");
    }

    auto kkvConfig = std::dynamic_pointer_cast<indexlibv2::config::KKVIndexConfig>(indexConfig);
    if (!kkvConfig) {
        return Status::InvalidArgs("failed to cast IIindexConfig to KKVIndexConfig");
    }
    const auto& indexName = kkvConfig->GetIndexName();
    auto valueConfig = kkvConfig->GetValueConfig();
    assert(valueConfig);

    // always pack
    auto packAttrFormatter = std::make_shared<indexlibv2::index::PackAttributeFormatter>();
    auto [status, packAttrConfig] = valueConfig->CreatePackAttributeConfig();
    RETURN_IF_STATUS_ERROR(status, "create pack attribute config fail, indexName[%s]", indexName.c_str());
    if (!packAttrFormatter->Init(packAttrConfig)) {
        return Status::InvalidArgs("pack attribute formatter init failed");
    }

    AUTIL_LOG(DEBUG, "QueryIndex, indexName=%s, pk=%s", indexName.c_str(), query.pk(0).c_str());

    auto kkvReader = GetIndexReader<KKVReader>("kkv", indexName);
    if (!kkvReader) {
        return Status::InvalidArgs("failed to get kkv reader, indexName[%s]", indexName.c_str());
    }
    auto attrs = ValidateAttrs(kkvReader, std::vector<std::string> {query.attrs().begin(), query.attrs().end()});
    autil::StringView pkey = query.pk(0);
    std::vector<autil::StringView> skeys(query.skey().begin(), query.skey().end());
    return FillRows(kkvReader, attrs, pkey, skeys, packAttrFormatter, partitionResponse);
}

Status KKVTabletReader::FillRows(const std::shared_ptr<KKVReader>& kkvReader, std::vector<std::string>& attrs,
                                 const autil::StringView& pkey, const std::vector<autil::StringView>& skeys,
                                 const std::shared_ptr<indexlibv2::index::PackAttributeFormatter>& packAttrFormatter,
                                 base::PartitionResponse& partitionResponse) const
{
    autil::mem_pool::Pool pool;
    KKVReadOptions readOptions;
    readOptions.pool = &pool;

    auto kkvDocIter = kkvReader->Lookup(pkey, skeys, readOptions);
    while (kkvDocIter && kkvDocIter->IsValid()) {
        FillRow(pkey, kkvReader, kkvDocIter, readOptions, attrs, packAttrFormatter, partitionResponse);
        kkvDocIter->MoveToNext();
    }
    if (kkvDocIter->IsOptimizeStoreSKey()) {
        // skey value already add to end at FillRow()
        auto skeyName = kkvReader->GetIndexConfig()->GetSuffixFieldConfig()->GetFieldName();
        attrs.push_back(skeyName);
    }
    indexlib::IE_POOL_COMPATIBLE_DELETE_CLASS(readOptions.pool, kkvDocIter);

    indexlib::index::AttrHelper::FillAttrInfo(attrs, partitionResponse);
    return Status::OK();
}

void KKVTabletReader::FillRow(const autil::StringView& pkey, const std::shared_ptr<KKVReader>& kkvReader,
                              index::KKVIterator* iter, indexlibv2::table::KKVReadOptions& readOptions,
                              const std::vector<std::string>& attrs,
                              const std::shared_ptr<indexlibv2::index::PackAttributeFormatter>& packAttrFormatter,
                              base::PartitionResponse& partitionResponse) const
{
    auto row = partitionResponse.add_rows();
    row->set_pk(std::string(pkey.data(), pkey.size()));

    autil::StringView value;
    iter->GetCurrentValue(value);

    using namespace indexlib::index;
    for (const auto& fieldName : attrs) {
        auto attrValue = row->add_attrvalues();
        AttrHelper::GetAttributeValue(value, packAttrFormatter, fieldName, *attrValue);
    }

    if (iter->IsOptimizeStoreSKey()) {
        auto skey = iter->GetCurrentSkey();
        FieldType skeyDictType = kkvReader->GetIndexConfig()->GetSuffixFieldConfig()->GetFieldType();
        auto attrValue = row->add_attrvalues();
        GetKkvSkeyValue(skey, skeyDictType, *attrValue);
    }
}

std::vector<std::string> KKVTabletReader::ValidateAttrs(const std::shared_ptr<KKVReader>& kkvReader,
                                                        const std::vector<std::string>& inputAttrs) const
{
    std::vector<std::string> kkvAttrs;
    auto valueConfig = kkvReader->GetIndexConfig()->GetValueConfig();
    size_t attrCount = valueConfig->GetAttributeCount();
    kkvAttrs.reserve(attrCount);
    for (size_t i = 0; i < attrCount; i++) {
        kkvAttrs.push_back(valueConfig->GetAttributeConfig(i)->GetAttrName());
    }
    return indexlib::index::AttrHelper::ValidateAttrs(kkvAttrs, inputAttrs);
}

#define FILL_SIMPLE_NUMBER_ATTRIBUTE(ft, strValueType)                                                                 \
    case ft: {                                                                                                         \
        attrValue.set_type(indexlib::index::AttrTypeTraits2<ft>::valueType);                                           \
        typedef indexlib::index::FieldTypeTraits<ft>::AttrItemType type;                                               \
        attrValue.set_##strValueType(static_cast<type>(value));                                                        \
        break;                                                                                                         \
    }

void KKVTabletReader::GetKkvSkeyValue(uint64_t value, FieldType fieldType, indexlibv2::base::AttrValue& attrValue)
{
    switch (fieldType) {
        FILL_SIMPLE_NUMBER_ATTRIBUTE(ft_int8, int32_value)
        FILL_SIMPLE_NUMBER_ATTRIBUTE(ft_uint8, uint32_value)
        FILL_SIMPLE_NUMBER_ATTRIBUTE(ft_int16, int32_value)
        FILL_SIMPLE_NUMBER_ATTRIBUTE(ft_uint16, uint32_value)
        FILL_SIMPLE_NUMBER_ATTRIBUTE(ft_int32, int32_value)
        FILL_SIMPLE_NUMBER_ATTRIBUTE(ft_uint32, uint32_value)
        FILL_SIMPLE_NUMBER_ATTRIBUTE(ft_int64, int64_value)
        FILL_SIMPLE_NUMBER_ATTRIBUTE(ft_uint64, uint64_value)
    default:
        // for kkv skey, float|double|string is not allowed
        // other FieldType should not appear hear
        break;
    }
#undef FILL_SIMPLE_NUMBER_ATTRIBUTE
}

} // namespace indexlibv2::table
