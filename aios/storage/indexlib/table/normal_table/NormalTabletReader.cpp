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
#include "indexlib/table/normal_table/NormalTabletReader.h"

#include "indexlib/config/MutableJson.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/ITabletReader.h"
#include "indexlib/framework/ResourceMap.h"
#include "indexlib/index/IIndexFactory.h"
#include "indexlib/index/IIndexReader.h"
#include "indexlib/index/IndexerParameter.h"
#include "indexlib/index/attribute/AttributeReader.h"
#include "indexlib/index/deletionmap/Common.h"
#include "indexlib/index/deletionmap/DeletionMapConfig.h"
#include "indexlib/index/inverted_index/IndexAccessoryReader.h"
#include "indexlib/index/inverted_index/MultiFieldIndexReader.h"
#include "indexlib/index/inverted_index/builtin_index/date/DateIndexReader.h"
#include "indexlib/index/inverted_index/builtin_index/range/RangeIndexReader.h"
#include "indexlib/index/inverted_index/builtin_index/spatial/SpatialIndexReader.h"
#include "indexlib/index/pack_attribute/Common.h"
#include "indexlib/index/pack_attribute/PackAttributeReader.h"
#include "indexlib/index/summary/Common.h"
#include "indexlib/index/summary/SummaryReader.h"
#include "indexlib/index/summary/config/SummaryIndexConfig.h"
#include "indexlib/table/normal_table/Common.h"
#include "indexlib/table/normal_table/DimensionDescription.h"
#include "indexlib/table/normal_table/DocRangePartitioner.h"
#include "indexlib/table/normal_table/NormalTabletInfo.h"
#include "indexlib/table/normal_table/NormalTabletInfoHolder.h"
#include "indexlib/table/normal_table/NormalTabletMeta.h"
#include "indexlib/table/normal_table/NormalTabletMetrics.h"
#include "indexlib/table/normal_table/NormalTabletSearcher.h"
#include "indexlib/table/normal_table/SortedDocIdRangeSearcher.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, NormalTabletReader);

NormalTabletReader::NormalTabletReader(const std::shared_ptr<config::ITabletSchema>& schema,
                                       const std::shared_ptr<NormalTabletMetrics>& normalTabletMetrics)
    : TabletReader(schema)
    , _normalTabletMetrics(normalTabletMetrics)
{
}

Status NormalTabletReader::Open(const std::shared_ptr<framework::TabletData>& tabletData,
                                const framework::ReadResource& readResource)
{
    index::IndexerParameter indexerParameter;
    RETURN_IF_STATUS_ERROR(PrepareIndexerParameter(readResource, indexerParameter), "prepare indexer parameter failed");
    auto indexFactoryCreator = index::IndexFactoryCreator::GetInstance();
    auto indexConfigs = _schema->GetIndexConfigs();
    for (const auto& indexConfig : indexConfigs) {
        const auto& indexType = indexConfig->GetIndexType();
        const auto& indexName = indexConfig->GetIndexName();
        auto [status, indexFactory] = indexFactoryCreator->Create(indexType);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "create index factory for index type [%s] failed, error: %s", indexType.c_str(),
                      status.ToString().c_str());
            return status;
        }
        auto indexReader = indexFactory->CreateIndexReader(indexConfig, indexerParameter);
        if (!indexReader) {
            AUTIL_LOG(ERROR, "create index reader [%s] failed", indexName.c_str());
            return Status::Corruption("create index reader failed");
        }
        status = indexReader->Open(indexConfig, tabletData.get());
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "create indexReader IndexType[%s] indexName[%s] failed, status[%s].", indexType.c_str(),
                      indexName.c_str(), status.ToString().c_str());
            return status;
        }
        _indexReaderMap[std::pair(indexType, indexName)] = std::move(indexReader);
    }
    auto status = InitInvertedIndexReaders();
    RETURN_IF_STATUS_ERROR(status, "init multi field index reader failed.");
    status = InitIndexAccessoryReader(tabletData.get(), readResource);
    RETURN_IF_STATUS_ERROR(status, "init index accessory reader failed.");
    _multiFieldIndexReader->SetAccessoryReader(_indexAccessoryReader);
    if (_deletionMapReader == nullptr) {
        _deletionMapReader = GetIndexReader<index::DeletionMapIndexReader>(index::DELETION_MAP_INDEX_TYPE_STR,
                                                                           index::DELETION_MAP_INDEX_NAME);
    }

    RETURN_IF_STATUS_ERROR(InitNormalTabletInfoHolder(tabletData, readResource), "init tablet info failed");

    _version = tabletData->GetOnDiskVersion().Clone();
    assert(_normalTabletMeta);
    status = InitSortedDocidRangeSearcher();
    RETURN_IF_STATUS_ERROR(status, "init sorted docid range searcher failed.");

    std::string indexType = index::PRIMARY_KEY_INDEX_TYPE_STR;
    auto pkConfigs = _schema->GetIndexConfigs(indexType);
    if (pkConfigs.size() > 1) {
        AUTIL_LOG(ERROR, "invalid pk config size [%lu]", pkConfigs.size());
        assert(false);
        return Status::Corruption("invalid pk config size ", pkConfigs.size());
    }
    if (pkConfigs.size() == 1) {
        auto pkConfig = pkConfigs[0];
        if (_primaryKeyIndexReader == nullptr) {
            _primaryKeyIndexReader = GetIndexReader<indexlib::index::PrimaryKeyIndexReader>(
                index::PRIMARY_KEY_INDEX_TYPE_STR, pkConfig->GetIndexName());
        }
    }
    // summary reader need pkReader && attribute reader
    status = InitSummaryReader();
    RETURN_IF_STATUS_ERROR(status, "init summary reader failed.");
    return status;
}

Status NormalTabletReader::InitNormalTabletInfoHolder(const std::shared_ptr<framework::TabletData>& tabletData,
                                                      const framework::ReadResource& readResource)
{
    auto resourceMap = tabletData->GetResourceMap();
    if (resourceMap == nullptr) {
        AUTIL_LOG(ERROR, "resource map is empty");
        return Status::InternalError("resource map is empty");
    }
    auto resource = resourceMap->GetResource(NORMAL_TABLET_INFO_HOLDER);
    std::shared_ptr<NormalTabletInfoHolder> lastNormalTabletInfoHolder;
    if (resource) {
        lastNormalTabletInfoHolder = std::dynamic_pointer_cast<NormalTabletInfoHolder>(resource);
        if (!lastNormalTabletInfoHolder) {
            RETURN_STATUS_ERROR(Corruption, "get normal tablet info holder failed");
        }
    }

    std::shared_ptr<index::DeletionMapIndexReader> delReader;
    std::string indexType = index::DELETION_MAP_INDEX_TYPE_STR;
    auto config = _schema->GetIndexConfig(indexType, index::DELETION_MAP_INDEX_NAME);
    if (config) {
        auto delConfig = std::dynamic_pointer_cast<index::DeletionMapConfig>(config);
        assert(delConfig);
        index::IndexerParameter indexerParam;
        RETURN_IF_STATUS_ERROR(PrepareIndexerParameter(readResource, indexerParam), "prepare indexer parameter failed");
        auto indexFactoryCreator = index::IndexFactoryCreator::GetInstance();
        auto [status, indexFactory] = indexFactoryCreator->Create(indexType);
        RETURN_IF_STATUS_ERROR(status, "create index factory for index type [%s] failed", indexType.c_str());
        auto indexReader = indexFactory->CreateIndexReader(delConfig, indexerParam);
        if (!indexReader) {
            AUTIL_LOG(ERROR, "create deletion map reader fail");
            return Status::InternalError();
        }
        // Create an independent deletionmap reader for normal tablet info.
        // Normal tablet info exists in resource map, it's lifetime is longer than reader.
        // If using deletionmap reader from TabletReader, when TabletReader deconstructs,
        // deletionmap reader's use_count is 2, while other index reader's use_count is 1.
        RETURN_IF_STATUS_ERROR(indexReader->Open(delConfig, tabletData.get()), "deletion map reader open fail");
        std::shared_ptr<index::IIndexReader> reader = std::move(indexReader);
        delReader = std::dynamic_pointer_cast<index::DeletionMapIndexReader>(reader);
        assert(delReader);
    }
    _normalTabletInfoHolder =
        NormalTabletInfoHolder::CreateOrReinit(lastNormalTabletInfoHolder, tabletData, _normalTabletMeta, delReader);
    if (!resource) {
        RETURN_IF_STATUS_ERROR(resourceMap->AddInheritedResource(NORMAL_TABLET_INFO_HOLDER, _normalTabletInfoHolder),
                               "add inhert resource fail, name[%s]", NORMAL_TABLET_INFO_HOLDER.c_str());
    }
    return Status::OK();
}

std::shared_ptr<indexlib::index::InvertedIndexReader> NormalTabletReader::GetMultiFieldIndexReader() const
{
    return _multiFieldIndexReader;
}

std::shared_ptr<NormalTabletInfo> NormalTabletReader::GetNormalTabletInfo() const
{
    return _normalTabletInfoHolder->GetNormalTabletInfo();
}

const framework::Version& NormalTabletReader::GetVersion() const { return _version; }

Status NormalTabletReader::InitSortedDocidRangeSearcher()
{
    autil::ScopedTime2 timer;
    AUTIL_LOG(INFO, "InitSortedDocidRangeSearcher begin");
    auto normalTabletMeta = _normalTabletInfoHolder->GetNormalTabletInfo()->GetTabletMeta();
    assert(normalTabletMeta);
    const config::SortDescriptions& sortDescs = normalTabletMeta->GetSortDescriptions();

    std::unordered_map<std::string, std::shared_ptr<index::AttributeReader>> sortAttrReaderMap;
    for (size_t i = 0; i < sortDescs.size(); i++) {
        const std::string& sortFieldName = sortDescs[i].GetSortFieldName();
        auto attrConfig = _schema->GetIndexConfig(index::ATTRIBUTE_INDEX_TYPE_STR, sortFieldName);
        if (!attrConfig) {
            AUTIL_LOG(ERROR, "get attribute config fail, indexName[%s]", sortFieldName.c_str());
            return Status::InternalError();
        }
        auto typedAttrConfig = std::dynamic_pointer_cast<indexlibv2::index::AttributeConfig>(attrConfig);
        assert(typedAttrConfig);
        if (!typedAttrConfig->IsDisabled()) {
            auto attrReader = GetIndexReader<index::AttributeReader>(typedAttrConfig->GetIndexType(), sortFieldName);
            if (!attrReader) {
                AUTIL_LOG(ERROR, "attribute reader not found, indexType[%s] indexName[%s]",
                          typedAttrConfig->GetIndexType().c_str(), sortFieldName.c_str());
                return Status::InternalError();
            }
            sortAttrReaderMap.emplace(sortFieldName, attrReader);
            AUTIL_LOG(INFO, "sort range search init attribute [%s]", sortFieldName.c_str());
        }
    }
    _sortedDocIdRangeSearcher = std::make_shared<SortedDocIdRangeSearcher>(sortAttrReaderMap, *normalTabletMeta);
    AUTIL_LOG(INFO, "InitSortedDocidRangeSearcher end, used[%.3f]s", timer.done_sec());
    return Status::OK();
}

Status NormalTabletReader::InitInvertedIndexReaders()
{
    auto* accessCounter = _normalTabletMetrics ? &_normalTabletMetrics->GetInvertedAccessCounter() : nullptr;
    _multiFieldIndexReader = std::make_shared<indexlib::index::MultiFieldIndexReader>(accessCounter);
    auto indexConfigs = _schema->GetIndexConfigs();
    for (const auto& indexConfig : indexConfigs) {
        auto indexType = indexConfig->GetIndexType();
        auto indexName = indexConfig->GetIndexName();
        auto iter = _indexReaderMap.find(std::pair(indexType, indexName));
        if (iter == _indexReaderMap.end()) {
            continue;
        }
        const auto& indexReader = iter->second;
        // TODO(yonghao.fyh) : confirm dynamic cast is legal
        auto reader = std::dynamic_pointer_cast<indexlib::index::InvertedIndexReader>(indexReader);
        auto config = std::dynamic_pointer_cast<indexlibv2::config::InvertedIndexConfig>(indexConfig);
        if (reader && config) {
            // check whether disabled or deleted
            if (!config->IsNormal()) {
                continue;
            }
            if (config->GetShardingType() == indexlibv2::config::InvertedIndexConfig::IST_IS_SHARDING) {
                continue;
            }
            InvertedIndexType type = config->GetInvertedIndexType();
            assert(type != it_kkv && type != it_kv);
            if (type == it_range) {
                AddAttributeReader<indexlib::index::RangeIndexReader>(reader, config);
            } else if (type == it_date) {
                AddAttributeReader<indexlib::index::DateIndexReader>(reader, config);
            } else if (type == it_spatial) {
                AddAttributeReader<indexlib::index::SpatialIndexReader>(reader, config);
            }
            Status status = _multiFieldIndexReader->AddIndexReader(config.get(), reader);
            if (!status.IsOK()) {
                AUTIL_LOG(ERROR, "MultiFieldIndexReader add index reader failed, IndexType[%s], IndexName[%s]",
                          indexType.c_str(), indexName.c_str());
                return status;
            }
        }
    }
    return Status::OK();
}

Status NormalTabletReader::InitSummaryReader()
{
    const auto& summaryIndexConfig = std::dynamic_pointer_cast<config::SummaryIndexConfig>(
        _schema->GetIndexConfig(index::SUMMARY_INDEX_TYPE_STR, index::SUMMARY_INDEX_NAME));
    if (nullptr == summaryIndexConfig) {
        return Status::OK(); // there is no summary here
    }
    auto summaryReader = GetIndexReader<index::SummaryReader>(index::SUMMARY_INDEX_TYPE_STR, index::SUMMARY_INDEX_NAME);
    if (nullptr == summaryReader) {
        RETURN_STATUS_ERROR(InternalError, "can not find summary reader");
    }
    // set primary key reader
    summaryReader->SetPrimaryKeyReader(_primaryKeyIndexReader.get());

    // add attribute reader
    const auto& attrIndexConfigs = _schema->GetIndexConfigs(index::ATTRIBUTE_INDEX_TYPE_STR);
    for (auto& attrIndexConfig : attrIndexConfigs) {
        auto attrConfig = std::dynamic_pointer_cast<indexlibv2::index::AttributeConfig>(attrIndexConfig);
        assert(attrConfig != nullptr);
        auto fieldId = attrConfig->GetFieldId();
        if (!summaryIndexConfig->IsInSummary(fieldId)) {
            continue;
        }
        std::string fieldName = attrConfig->GetAttrName();
        auto attrReader = GetIndexReader<index::AttributeReader>(index::ATTRIBUTE_INDEX_TYPE_STR, fieldName);
        if (nullptr == attrReader) {
            RETURN_STATUS_ERROR(InternalError, "can not find attribute reader for field [%s]", fieldName.c_str());
        }
        summaryReader->AddAttrReader(fieldId, attrReader.get());
    }

    // add pack attribute reader
    const auto& packAttrIndexConfigs = _schema->GetIndexConfigs(index::PACK_ATTRIBUTE_INDEX_TYPE_STR);
    for (const auto& packAttrIndexConfig : packAttrIndexConfigs) {
        const auto& packName = packAttrIndexConfig->GetIndexName();
        const auto& fieldConfigs = packAttrIndexConfig->GetFieldConfigs();
        for (const auto& fieldConfig : fieldConfigs) {
            auto fieldId = fieldConfig->GetFieldId();
            if (!summaryIndexConfig->IsInSummary(fieldId)) {
                continue;
            }
            auto packAttrReader =
                GetIndexReader<index::PackAttributeReader>(index::PACK_ATTRIBUTE_INDEX_TYPE_STR, packName);
            if (!packAttrReader) {
                RETURN_STATUS_ERROR(InternalError, "can not find pack attribute reader  [%s]", packName.c_str());
            }
            summaryReader->AddPackAttrReader(fieldId, packAttrReader.get());
        }
    }
    return Status::OK();
}

const std::shared_ptr<index::DeletionMapIndexReader>& NormalTabletReader::GetDeletionMapReader() const
{
    return _deletionMapReader;
}
const std::shared_ptr<indexlib::index::PrimaryKeyIndexReader>& NormalTabletReader::GetPrimaryKeyReader() const
{
    return _primaryKeyIndexReader;
}

std::shared_ptr<index::SummaryReader> NormalTabletReader::GetSummaryReader() const
{
    return GetIndexReader<index::SummaryReader>(index::SUMMARY_INDEX_TYPE_STR, index::SUMMARY_INDEX_NAME);
}

template <typename T>
void NormalTabletReader::AddAttributeReader(const std::shared_ptr<indexlib::index::InvertedIndexReader>& indexReader,
                                            const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig)
{
    auto singleFieldConfig = std::dynamic_pointer_cast<indexlibv2::config::SingleFieldIndexConfig>(indexConfig);
    assert(singleFieldConfig);
    const auto& fieldName = singleFieldConfig->GetFieldConfig()->GetFieldName();
    auto attrReader = GetIndexReader<index::AttributeReader>(index::ATTRIBUTE_INDEX_TYPE_STR, fieldName);

    // TODO: multi value attribute reader
    auto compositeReader = std::dynamic_pointer_cast<T>(indexReader);
    assert(compositeReader);
    compositeReader->AddAttributeReader(attrReader.get());
}

Status NormalTabletReader::InitIndexAccessoryReader(const framework::TabletData* tabletData,
                                                    const framework::ReadResource& readResource)
{
    if (_schema->GetIndexConfigs(indexlib::index::GENERAL_INVERTED_INDEX_TYPE_STR).size() <= 0) {
        return Status::OK();
    }
    index::IndexerParameter indexerParameter;
    RETURN_IF_STATUS_ERROR(PrepareIndexerParameter(readResource, indexerParameter), "prepare indexer parameter failed");
    std::vector<std::shared_ptr<config::IIndexConfig>> indexConfigs;
    for (const auto& originIndexConfig : _schema->GetIndexConfigs(indexlib::index::GENERAL_INVERTED_INDEX_TYPE_STR)) {
        auto config = std::dynamic_pointer_cast<indexlibv2::config::InvertedIndexConfig>(originIndexConfig);
        if (config == nullptr) {
            return Status::InvalidArgs("inverted index name[%s] invalid index config", config->GetIndexName().c_str());
        }
        if (config->GetShardingType() == indexlibv2::config::InvertedIndexConfig::IST_NEED_SHARDING) {
            indexConfigs.insert(indexConfigs.end(), config->GetShardingIndexConfigs().begin(),
                                config->GetShardingIndexConfigs().end());
        }
        indexConfigs.emplace_back(config);
    }

    _indexAccessoryReader = std::make_shared<indexlib::index::IndexAccessoryReader>(indexConfigs, indexerParameter);
    assert(_indexAccessoryReader);
    return _indexAccessoryReader->Open(tabletData);
}

std::shared_ptr<index::AttributeReader> NormalTabletReader::GetAttributeReader(const std::string& attrName) const
{
    if (_normalTabletMetrics) {
        const auto& accessCounter = _normalTabletMetrics->GetAttributeAccessCounter();
        auto iter = accessCounter.find(attrName);
        if (iter != accessCounter.end() && iter->second.get() != nullptr) {
            iter->second->Increase(1);
        }
    }
    return GetIndexReader<index::AttributeReader>(index::ATTRIBUTE_INDEX_TYPE_STR, attrName);
}

std::shared_ptr<index::PackAttributeReader>
NormalTabletReader::GetPackAttributeReader(const std::string& packName) const
{
    return GetIndexReader<index::PackAttributeReader>(index::PACK_ATTRIBUTE_INDEX_TYPE_STR, packName);
}

const NormalTabletReader::AccessCounterMap& NormalTabletReader::GetInvertedAccessCounter() const
{
    if (_normalTabletMetrics) {
        return _normalTabletMetrics->GetInvertedAccessCounter();
    }
    static NormalTabletReader::AccessCounterMap counters;
    return counters;
}
const NormalTabletReader::AccessCounterMap& NormalTabletReader::GetAttributeAccessCounter() const
{
    if (_normalTabletMetrics) {
        return _normalTabletMetrics->GetAttributeAccessCounter();
    }
    static NormalTabletReader::AccessCounterMap counters;
    return counters;
}

Status NormalTabletReader::PrepareIndexerParameter(const framework::ReadResource& readResource,
                                                   index::IndexerParameter& indexerParameter)
{
    auto [status, descs] = _schema->GetRuntimeSettings().GetValue<config::SortDescriptions>("sort_descriptions");
    if (!status.IsOK() && !status.IsNotFound()) {
        AUTIL_LOG(ERROR, "parse sort descs from schema has error");
        return status;
    }
    auto normalTabletMeta = std::make_shared<NormalTabletMeta>(descs);
    indexerParameter.metricsManager = readResource.metricsManager;
    indexerParameter.sortPatternFunc = [normalTabletMeta](const std::string& name) -> config::SortPattern {
        return normalTabletMeta->GetSortPattern(name);
    };
    _normalTabletMeta = normalTabletMeta;
    return Status::OK();
}

bool NormalTabletReader::GetSortedDocIdRanges(
    const std::vector<std::shared_ptr<indexlib::table::DimensionDescription>>& dimensions,
    const DocIdRange& rangeLimits, DocIdRangeVector& resultRanges) const
{
    return _sortedDocIdRangeSearcher->GetSortedDocIdRanges(dimensions, rangeLimits, resultRanges);
}

bool NormalTabletReader::GetPartedDocIdRanges(const DocIdRangeVector& rangeHint, size_t totalWayCount, size_t wayIdx,
                                              DocIdRangeVector& ranges) const
{
    return DocRangePartitioner::GetPartedDocIdRanges(
        rangeHint, _normalTabletInfoHolder->GetNormalTabletInfo()->GetIncDocCount(), totalWayCount, wayIdx, ranges);
}

bool NormalTabletReader::GetPartedDocIdRanges(const DocIdRangeVector& rangeHint, size_t totalWayCount,
                                              std::vector<DocIdRangeVector>& rangeVectors) const
{
    return DocRangePartitioner::GetPartedDocIdRanges(
        rangeHint, _normalTabletInfoHolder->GetNormalTabletInfo()->GetIncDocCount(), totalWayCount, rangeVectors);
}

Status NormalTabletReader::Search(const std::string& jsonQuery, std::string& result) const
{
    return NormalTabletSearcher(this).Search(jsonQuery, result);
}

Status NormalTabletReader::QueryIndex(const base::PartitionQuery& query,
                                      base::PartitionResponse& partitionResponse) const
{
    return NormalTabletSearcher(this).QueryIndex(query, partitionResponse);
}

Status NormalTabletReader::QueryDocIds(const base::PartitionQuery& query, base::PartitionResponse& partitionResponse,
                                       std::vector<docid_t>& docids) const
{
    return NormalTabletSearcher(this).QueryDocIds(query, partitionResponse, docids);
}

} // namespace indexlibv2::table
