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
#pragma once

#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/index/normal/attribute/accessor/patch_apply_option.h"
#include "indexlib/index/normal/attribute/accessor/primary_key_attribute_reader.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader_adaptor.h"
#include "indexlib/index/normal/primarykey/legacy_primary_key_reader_interface.h"
#include "indexlib/index/normal/primarykey/segment_data_adapter.h"
#include "indexlib/index/primary_key/PrimaryKeyReader.h"
#include "indexlib/index_base/index_meta/temperature_doc_info.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/segment_iterator.h"

namespace indexlib::index {
template <typename Key>
class LegacyPrimaryKeyReader : public indexlibv2::index::PrimaryKeyReader<Key, LegacyPrimaryKeyReader<Key>>,
                               public LegacyPrimaryKeyReaderInterface
{
public:
    LegacyPrimaryKeyReader() : indexlibv2::index::PrimaryKeyReader<Key, LegacyPrimaryKeyReader<Key>>(nullptr) {}

public:
    Status Open(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                const indexlibv2::framework::TabletData* tabletData) override;

    Status OpenWithoutPKAttribute(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                                  const indexlibv2::framework::TabletData* tabletData) override;

    void Open(const indexlib::config::IndexConfigPtr& indexConfig,
              const indexlib::index_base::PartitionDataPtr& partitionData,
              const InvertedIndexReader* hintReader = nullptr) override;

    void OpenWithoutPKAttribute(const indexlib::config::IndexConfigPtr& indexConfig,
                                const indexlib::index_base::PartitionDataPtr& partitionData,
                                bool forceReverseLookup) override;
    size_t EstimateLoadSize(const indexlib::index_base::PartitionDataPtr& partitionData,
                            const indexlib::config::IndexConfigPtr& indexConfig,
                            const indexlib::index_base::Version& lastLoadVersion) override;

    indexlib::index::AttributeReaderPtr GetLegacyPKAttributeReader() const override { return _pkAttributeReader; }

public:
    void SetMultiFieldIndexReader(InvertedIndexReader* truncateReader) override {}
    void InitBuildResourceMetricsNode(util::BuildResourceMetrics* buildResourceMetrics) override {}
    void SetLegacyAccessoryReader(const std::shared_ptr<LegacyIndexAccessoryReader>& accessorReader) override {}
    std::shared_ptr<LegacyIndexAccessoryReader> GetLegacyAccessoryReader() const override { return nullptr; }

    void Update(docid_t docId, const document::ModifiedTokens& tokens) override { assert(false); }
    void Update(docid_t docId, index::DictKeyInfo termKey, bool isDelete) override { assert(false); }
    // UpdateIndex is used to update patch format index.
    void UpdateIndex(IndexUpdateTermIterator* iter) override { assert(false); }

public:
    bool IsDocDeleted(docid_t docid) const { return _deletionMapReader && _deletionMapReader->IsDeleted(docid); }
    bool GetDocIdRanges(int32_t hintValues, DocIdRangeVector& docIdRanges) const;

private:
    using Base = indexlibv2::index::PrimaryKeyReader<Key, LegacyPrimaryKeyReader<Key>>;

private:
    [[nodiscard]] bool DoOpenPKAttribute(const indexlib::config::IndexConfigPtr& indexConfig,
                                         const indexlib::index_base::PartitionDataPtr& partitionData);
    void InitBuildingIndexReader(const indexlib::index_base::PartitionDataPtr& partitionData);

    [[nodiscard]] inline size_t
    DoEstimateLoadSize(std::vector<indexlibv2::index::SegmentDataAdapter::SegmentDataType>& segmentDatas,
                       const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                       const indexlib::index_base::Version& lastLoadVersion);

protected:
    std::shared_ptr<DeletionMapReader> _deletionMapReader;

private:
    std::shared_ptr<PrimaryKeyAttributeReader<Key>> _pkAttributeReader;
    indexlib::index_base::TemperatureDocInfo* _temperatureDocInfo = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<PrimaryKeyIndexReader> PrimaryKeyIndexReaderPtr;

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, LegacyPrimaryKeyReader, T);

typedef LegacyPrimaryKeyReader<uint64_t> UInt64LegacyPrimaryKeyReader;
typedef std::shared_ptr<UInt64LegacyPrimaryKeyReader> UInt64LegacyPrimaryKeyReaderPtr;
typedef LegacyPrimaryKeyReader<autil::uint128_t> UInt128LegacyPrimaryKeyReader;
typedef std::shared_ptr<UInt128LegacyPrimaryKeyReader> UInt128LegacyPrimaryKeyReaderPtr;

template <typename Key>
inline void LegacyPrimaryKeyReader<Key>::Open(const indexlib::config::IndexConfigPtr& indexConfig,
                                              const indexlib::index_base::PartitionDataPtr& partitionData,
                                              const InvertedIndexReader* hintReader)
{
    Base::_indexConfig = indexConfig;
    auto legacyPrimaryKeyIndexConfig = DYNAMIC_POINTER_CAST(indexlib::config::PrimaryKeyIndexConfig, indexConfig);
    assert(legacyPrimaryKeyIndexConfig);
    Base::_primaryKeyIndexConfig = legacyPrimaryKeyIndexConfig->MakePrimaryIndexConfigV2();
    assert(Base::_primaryKeyIndexConfig);
    _temperatureDocInfo = partitionData->GetTemperatureDocInfo().get();
    OpenWithoutPKAttribute(indexConfig, partitionData, false);
    if (!DoOpenPKAttribute(indexConfig, partitionData)) {
        AUTIL_LEGACY_THROW(indexlib::util::RuntimeException, "DoOpenPKAttribute has exception");
    }
}

template <typename Key>
inline void
LegacyPrimaryKeyReader<Key>::OpenWithoutPKAttribute(const indexlib::config::IndexConfigPtr& indexConfig,
                                                    const indexlib::index_base::PartitionDataPtr& partitionData,
                                                    bool forceReverseLookup)
{
    Base::_indexConfig = indexConfig;
    auto legacyPrimaryKeyIndexConfig = DYNAMIC_POINTER_CAST(indexlib::config::PrimaryKeyIndexConfig, indexConfig);
    assert(legacyPrimaryKeyIndexConfig);
    Base::_primaryKeyIndexConfig = legacyPrimaryKeyIndexConfig->MakePrimaryIndexConfigV2();
    assert(Base::_primaryKeyIndexConfig);

    Base::InnerInit();
    indexlib::index::SegmentDataAdapter::Transform(partitionData, Base::_segmentDatas);

    if (!Base::DoOpen(Base::_segmentDatas, forceReverseLookup)) {
        AUTIL_LEGACY_THROW(indexlib::util::RuntimeException, "DoOpen has exception");
    }

    InitBuildingIndexReader(partitionData);

    try {
        _deletionMapReader = partitionData->GetDeletionMapReader();
    } catch (indexlib::util::UnSupportedException& e) {
        _deletionMapReader.reset();
        // todo remove this log later
        AUTIL_LOG(INFO, "Merger reclaim document will use MergePartitionData, "
                        "which not support get deletionMapReader.");
    }
}

template <typename Key>
inline bool LegacyPrimaryKeyReader<Key>::DoOpenPKAttribute(const indexlib::config::IndexConfigPtr& indexConfig,
                                                           const indexlib::index_base::PartitionDataPtr& partitionData)
{
    assert(Base::_primaryKeyIndexConfig);
    auto legacyPrimaryKeyIndexConfig = DYNAMIC_POINTER_CAST(indexlib::config::PrimaryKeyIndexConfig, indexConfig);
    assert(legacyPrimaryKeyIndexConfig);
    if (legacyPrimaryKeyIndexConfig->HasPrimaryKeyAttribute()) {
        _pkAttributeReader.reset(new PrimaryKeyAttributeReader<Key>(legacyPrimaryKeyIndexConfig->GetIndexName()));
        indexlib::config::AttributeConfigPtr attrConfig(new indexlib::config::AttributeConfig());
        attrConfig->Init(legacyPrimaryKeyIndexConfig->GetFieldConfig());
        try {
            _pkAttributeReader->Open(attrConfig, partitionData,
                                     indexlib::index::PatchApplyStrategy::PAS_APPLY_NO_PATCH);
        } catch (...) {
            AUTIL_LOG(ERROR, "pkAttributeReader Open failed");
            return false;
        }
    }
    return true;
}

template <typename Key>
inline void
LegacyPrimaryKeyReader<Key>::InitBuildingIndexReader(const indexlib::index_base::PartitionDataPtr& partitionData)
{
    indexlib::index_base::PartitionSegmentIteratorPtr segIter = partitionData->CreateSegmentIterator();
    assert(segIter);
    indexlib::index_base::SegmentIteratorPtr memIndexerIter =
        segIter->CreateIterator(indexlib::index_base::SIT_BUILDING);

    while (memIndexerIter->IsValid()) {
        indexlib::index_base::InMemorySegmentPtr inMemSegment = memIndexerIter->GetInMemSegment();
        if (!inMemSegment) {
            memIndexerIter->MoveToNext();
            continue;
        }

        const indexlib::index_base::InMemorySegmentReaderPtr& inMemSegReader = inMemSegment->GetSegmentReader();
        if (!inMemSegReader) {
            memIndexerIter->MoveToNext();
            continue;
        }
        assert(Base::_primaryKeyIndexConfig);
        std::shared_ptr<indexlib::index::IndexSegmentReader> pkIndexReader =
            inMemSegReader->GetMultiFieldIndexSegmentReader()->GetIndexSegmentReader(Base::GetIndexName());
        auto inMemPKSegReader =
            DYNAMIC_POINTER_CAST(indexlib::index::InMemPrimaryKeySegmentReaderTyped<Key>, pkIndexReader);
        if (!Base::_buildingIndexReader) {
            Base::_buildingIndexReader.reset(new indexlib::index::PrimaryKeyBuildingIndexReader<Key>());
        }
        Base::_buildingIndexReader->AddSegmentReader(memIndexerIter->GetBaseDocId(), inMemSegment->GetSegmentId(),
                                                     inMemPKSegReader->GetHashMap());
        AUTIL_LOG(INFO, "Add In-Memory SegmentReader for segment [%d], by pk index [%s]",
                  memIndexerIter->GetSegmentId(), Base::_primaryKeyIndexConfig->GetIndexName().c_str());
        memIndexerIter->MoveToNext();
    }
}

template <typename Key>
size_t LegacyPrimaryKeyReader<Key>::EstimateLoadSize(const indexlib::index_base::PartitionDataPtr& partitionData,
                                                     const indexlib::config::IndexConfigPtr& indexConfig,
                                                     const indexlib::index_base::Version& lastLoadVersion)
{
    std::vector<indexlibv2::index::SegmentDataAdapter::SegmentDataType> segmentDatas;
    indexlib::index::SegmentDataAdapter::Transform(partitionData, segmentDatas);
    auto pkIndexConfig = DYNAMIC_POINTER_CAST(indexlib::config::PrimaryKeyIndexConfig, indexConfig);
    assert(pkIndexConfig);
    bool isValidVersion = (lastLoadVersion != indexlib::index_base::Version(INVALID_VERSIONID));
    size_t totalMemUsed =
        Base::DoEstimateLoadSize(segmentDatas, pkIndexConfig->MakePrimaryIndexConfigV2(), isValidVersion);
    if (pkIndexConfig->HasPrimaryKeyAttribute()) {
        indexlib::index::AttributeReaderPtr attrReader(
            new PrimaryKeyAttributeReader<Key>(pkIndexConfig->GetIndexName()));
        indexlib::config::AttributeConfigPtr attrConfig(new indexlib::config::AttributeConfig());
        attrConfig->Init(pkIndexConfig->GetFieldConfig());
        totalMemUsed += attrReader->EstimateLoadSize(partitionData, attrConfig, lastLoadVersion);
    }
    return totalMemUsed;
}

template <typename Key>
inline bool LegacyPrimaryKeyReader<Key>::GetDocIdRanges(int32_t hintValues, DocIdRangeVector& docIdRanges) const
{
    if (_temperatureDocInfo && _temperatureDocInfo->GetTemperatureDocIdRanges(hintValues, docIdRanges)) {
        return true;
    }
    return false;
}
template <typename Key>
Status LegacyPrimaryKeyReader<Key>::Open(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                                         const indexlibv2::framework::TabletData* tabletData)
{
    assert(0);
    return Status::Unimplement();
}

template <typename Key>
Status LegacyPrimaryKeyReader<Key>::OpenWithoutPKAttribute(
    const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
    const indexlibv2::framework::TabletData* tabletData)
{
    assert(0);
    return Status::Unimplement();
}

} // namespace indexlib::index
