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
#include "indexlib/index/aggregation/AggregationReader.h"

#include <optional>
#include <queue>

#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"
#include "autil/result/Errors.h"
#include "indexlib/base/FieldTypeUtil.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/aggregation/AggregationDiskIndexer.h"
#include "indexlib/index/aggregation/AggregationKeyHasher.h"
#include "indexlib/index/aggregation/AggregationMemIndexer.h"
#include "indexlib/index/aggregation/MultiSegmentKeyIterator.h"
#include "indexlib/index/aggregation/SequentialMultiSegmentValueIterator.h"
#include "indexlib/index/common/field_format/pack_attribute/PackAttributeFormatter.h"
#include "indexlib/index/common/field_format/pack_attribute/PackValueComparator.h"
#include "indexlib/index/kv/config/ValueConfig.h"

namespace indexlibv2::index {
AUTIL_DECLARE_AND_SETUP_LOGGER(index, AggregationReader);

AggregationReader::AggregationReader() = default;

AggregationReader::~AggregationReader() = default;

Status AggregationReader::Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                               const framework::TabletData* tabletData)
{
    auto aggIndexConfig = std::dynamic_pointer_cast<config::AggregationIndexConfig>(indexConfig);
    if (!aggIndexConfig) {
        return Status::InternalError("expect AggregationIndexConfig");
    }

    std::vector<std::shared_ptr<framework::Segment>> memorySegments;
    std::vector<std::shared_ptr<framework::Segment>> diskSegments;

    auto slice = tabletData->CreateSlice(framework::Segment::SegmentStatus::ST_BUILDING);
    for (auto it = slice.rbegin(); it != slice.rend(); ++it) {
        memorySegments.push_back(*it);
    }

    slice = tabletData->CreateSlice(framework::Segment::SegmentStatus::ST_DUMPING);
    for (auto it = slice.rbegin(); it != slice.rend(); ++it) {
        memorySegments.push_back(*it);
    }

    slice = tabletData->CreateSlice(framework::Segment::SegmentStatus::ST_BUILT);
    for (auto it = slice.rbegin(); it != slice.rend(); ++it) {
        diskSegments.push_back(*it);
    }
    return Init(aggIndexConfig, memorySegments, diskSegments);
}

Status AggregationReader::Init(const std::shared_ptr<config::AggregationIndexConfig>& indexConfig,
                               const std::vector<std::shared_ptr<framework::Segment>>& memorySegments,
                               const std::vector<std::shared_ptr<framework::Segment>>& diskSegments)
{
    SegmentReaders segmentReaders, deleteReaders;
    segmentReaders.reserve(memorySegments.size() + diskSegments.size());
    deleteReaders.reserve(memorySegments.size() + diskSegments.size());
    auto s = CollectSegmentReaders<true>(indexConfig, memorySegments, segmentReaders, deleteReaders);
    RETURN_STATUS_DIRECTLY_IF_ERROR(s);

    s = CollectSegmentReaders<false>(indexConfig, diskSegments, segmentReaders, deleteReaders);
    RETURN_STATUS_DIRECTLY_IF_ERROR(s);

    s = Init(indexConfig, std::move(segmentReaders));
    RETURN_STATUS_DIRECTLY_IF_ERROR(s);

    if (indexConfig->SupportDelete()) {
        _deleteReader = std::make_shared<AggregationReader>();
        s = _deleteReader->Init(indexConfig->GetDeleteConfig(), std::move(deleteReaders));
    }
    return s;
}

Status AggregationReader::Init(const std::shared_ptr<config::AggregationIndexConfig>& indexConfig,
                               SegmentReaders segmentReaders)
{
    _segmentReaders = std::move(segmentReaders);

    _indexConfig = indexConfig;
    auto s = InitValueReference();
    if (!s.IsOK()) {
        return s;
    }

    const auto& sortDescriptions = _indexConfig->GetSortDescriptions();
    if (sortDescriptions.size() > 0) {
        _cmp = std::make_shared<PackValueComparator>();
        if (!_cmp->Init(_indexConfig->GetValueConfig(), sortDescriptions)) {
            return Status::InternalError("create PackValueComparator failed");
        }
    }
    return Status::OK();
}

std::unique_ptr<IKeyIterator> AggregationReader::CreateKeyIterator() const
{
    std::vector<std::unique_ptr<IKeyIterator>> iters;
    for (const auto& segReader : _segmentReaders) {
        iters.emplace_back(segReader->CreateKeyIterator());
    }
    auto iter = std::make_unique<MultiSegmentKeyIterator>();
    iter->Init(std::move(iters));
    return iter;
}

uint64_t AggregationReader::CalculateKeyHash(const std::vector<autil::StringView>& keys) const
{
    return AggregationKeyHasher::Hash(keys);
}

ValueMeta AggregationReader::GetMergedValueMeta(uint64_t key) const
{
    ValueMeta accumulated;
    for (const auto& segReader : _segmentReaders) {
        ValueMeta meta;
        if (segReader->GetValueMeta(key, meta)) {
            accumulated.valueCount += meta.valueCount;
        }
    }
    return accumulated;
}

Status AggregationReader::InitValueReference()
{
    auto valueConfig = _indexConfig->GetValueConfig();
    auto [s, packAttrConfig] = valueConfig->CreatePackAttributeConfig();
    if (!s.IsOK()) {
        return s;
    }
    _packAttrFormatter = std::make_unique<PackAttributeFormatter>();
    if (!_packAttrFormatter->Init(packAttrConfig)) {
        return Status::InternalError("create pack attribute formatter failed: %s",
                                     _indexConfig->GetIndexName().c_str());
    }
    return Status::OK();
}

template <bool inMemory>
Status AggregationReader::CollectSegmentReaders(const std::shared_ptr<config::AggregationIndexConfig>& indexConfig,
                                                const std::vector<std::shared_ptr<framework::Segment>>& segments,
                                                SegmentReaders& segmentReaders, SegmentReaders& deleteReaders)
{
    for (auto it = segments.begin(); it != segments.end(); it++) {
        const auto& segment = *it;
        auto [s, indexer] = segment->GetIndexer(indexConfig->GetIndexType(), indexConfig->GetIndexName());
        if (!s.IsOK()) {
            return s;
        }
        std::shared_ptr<ISegmentReader> reader;
        std::shared_ptr<ISegmentReader> deleteReader;
        if constexpr (inMemory) {
            auto memIndexer = std::dynamic_pointer_cast<AggregationMemIndexer>(indexer);
            if (!memIndexer) {
                return Status::InternalError("expect AggregationMemIndexer, actual: %s", typeid(indexer).name());
            }
            reader = memIndexer->CreateInMemoryReader();
            deleteReader = memIndexer->CreateInMemoryDeleteReader();
        } else {
            auto diskIndexer = std::dynamic_pointer_cast<AggregationDiskIndexer>(indexer);
            if (!diskIndexer) {
                return Status::InternalError("expect AggregationDiskIndexer, actual: %s", typeid(indexer).name());
            }
            reader = diskIndexer->GetReader();
            deleteReader = diskIndexer->GetDeleteReader();
        }
        if (!reader) {
            return Status::InternalError("create segment reader for segment %d failed", segment->GetSegmentId());
        }
        segmentReaders.emplace_back(std::move(reader));
        if (indexConfig->SupportDelete()) {
            if (!deleteReader) {
                return Status::InternalError("create delete reader for segment %d failed", segment->GetSegmentId());
            }
            deleteReaders.emplace_back(std::move(deleteReader));
        }
    }
    return Status::OK();
}

std::vector<std::unique_ptr<IValueIterator>> AggregationReader::CreateLeafIters(uint64_t key,
                                                                                autil::mem_pool::PoolBase* pool) const
{
    std::vector<std::unique_ptr<IValueIterator>> iters;
    iters.reserve(_segmentReaders.size());
    for (auto it = _segmentReaders.begin(); it != _segmentReaders.end(); ++it) {
        const auto& segReader = *it;
        auto valueIter = segReader->Lookup(key, pool);
        if (!valueIter) {
            continue;
        }
        if (auto multiIter = dynamic_cast<SequentialMultiSegmentValueIterator*>(valueIter.get())) {
            auto flatten = multiIter->Steal();
            for (auto& innerIter : flatten) {
                iters.emplace_back(std::move(innerIter));
            }
        } else {
            iters.emplace_back(std::move(valueIter));
        }
    }
    return iters;
}

} // namespace indexlibv2::index
