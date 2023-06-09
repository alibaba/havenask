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
#include "indexlib/index/kv/SingleShardKVIndexReader.h"

#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/kv/AdapterIgnoreFieldCalculator.h"
#include "indexlib/index/kv/KVDiskIndexer.h"
#include "indexlib/index/kv/KVMemIndexerBase.h"
#include "indexlib/index/kv/KVSegmentReaderCreator.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, SingleShardKVIndexReader);

SingleShardKVIndexReader::SingleShardKVIndexReader(schemaid_t readerSchemaId)
    : KVIndexReader(readerSchemaId)
    , _hasTTL(false)
    , _kvReportMetrics(true)
{
}

Status SingleShardKVIndexReader::DoOpen(const std::shared_ptr<indexlibv2::config::KVIndexConfig>& kvIndexConfig,
                                        const framework::TabletData* tabletData) noexcept
{
    if (tabletData->GetSegmentCount() == 0) {
        return Status::OK();
    }

    auto ignoreFieldCalculator = std::make_shared<index::AdapterIgnoreFieldCalculator>();
    if (!ignoreFieldCalculator->Init(kvIndexConfig, tabletData)) {
        return Status::InternalError("init AdapterIgnoreFieldCalculator fail for index [%s]",
                                     kvIndexConfig->GetIndexName().c_str());
    }

    Status s;
    s = LoadSegments(kvIndexConfig, ignoreFieldCalculator, tabletData, framework::Segment::SegmentStatus::ST_BUILDING);
    if (!s.IsOK()) {
        return s;
    }

    s = LoadSegments(kvIndexConfig, ignoreFieldCalculator, tabletData, framework::Segment::SegmentStatus::ST_DUMPING);
    if (!s.IsOK()) {
        return s;
    }

    return LoadSegments(kvIndexConfig, ignoreFieldCalculator, tabletData, framework::Segment::SegmentStatus::ST_BUILT);
}

Status
SingleShardKVIndexReader::LoadSegments(const std::shared_ptr<config::KVIndexConfig>& indexConfig,
                                       const std::shared_ptr<AdapterIgnoreFieldCalculator>& ignoreFieldCalculator,
                                       const framework::TabletData* tabletData,
                                       framework::Segment::SegmentStatus status)
{
    if (status == framework::Segment::SegmentStatus::ST_UNSPECIFY) {
        return Status::InvalidArgs("must be one of ST_BUILDING, ST_DUMPING, ST_BUILT");
    }
    auto slice = tabletData->CreateSlice(status);
    auto begin = slice.rbegin();
    auto end = slice.rend();
    if (status == framework::Segment::SegmentStatus::ST_BUILT) {
        return LoadSlice<decltype(begin), false>(indexConfig, ignoreFieldCalculator, begin, end);
    } else {
        return LoadSlice<decltype(begin), true>(indexConfig, ignoreFieldCalculator, begin, end);
    }
}

template <typename Iterator, bool inMemory>
Status SingleShardKVIndexReader::LoadSlice(const std::shared_ptr<config::KVIndexConfig>& indexConfig,
                                           const std::shared_ptr<AdapterIgnoreFieldCalculator>& ignoreFieldCalculator,
                                           Iterator begin, Iterator end)
{
    for (auto it = begin; it != end; it++) {
        auto segment = it->get();
        schemaid_t segSchemaId = segment->GetSegmentSchema()->GetSchemaId();
        auto ignoreFields = ignoreFieldCalculator->GetIgnoreFields(std::min(segSchemaId, _readerSchemaId),
                                                                   std::max(segSchemaId, _readerSchemaId));
        auto [s, indexer] = segment->GetIndexer(indexConfig->GetIndexType(), indexConfig->GetIndexName());
        if (!s.IsOK()) {
            return s;
        }

        std::shared_ptr<config::KVIndexConfig> segmentIndexConfig;
        std::shared_ptr<IKVSegmentReader> segmentReader;
        if constexpr (inMemory) {
            auto memIndexer = std::dynamic_pointer_cast<KVMemIndexerBase>(indexer);
            if (memIndexer) {
                segmentIndexConfig = memIndexer->GetIndexConfig();
                segmentReader = memIndexer->CreateInMemoryReader();
            }
        } else {
            auto diskIndexer = std::dynamic_pointer_cast<KVDiskIndexer>(indexer);
            if (diskIndexer) {
                segmentIndexConfig = diskIndexer->GetIndexConfig();
                segmentReader = diskIndexer->GetReader();
            }
        }
        if (!segmentReader) {
            return Status::InternalError("failed to create segment reader for %s in segment[%d]",
                                         indexConfig->GetIndexName().c_str(), segment->GetSegmentId());
        }
        if (segSchemaId != _readerSchemaId) {
            AUTIL_LOG(INFO,
                      "Begin Load AdapterSegmentReader for segment [%d], "
                      "segmentSchemaId [%d], readerSchemaId [%d]",
                      segment->GetSegmentId(), segSchemaId, _readerSchemaId);
            std::tie(s, segmentReader) = KVSegmentReaderCreator::CreateSegmentReader(segmentReader, segmentIndexConfig,
                                                                                     indexConfig, ignoreFields, true);
            if (!s.IsOK()) {
                return s;
            }
        }
        if constexpr (inMemory) {
            _memorySegmentReaders.emplace_back(std::move(segmentReader));
        } else {
            _diskSegmentReaders.emplace_back(std::move(segmentReader));
            _diskSegmentLocators.emplace_back(
                std::make_shared<framework::Locator>(segment->GetSegmentInfo()->GetLocator()));
        }
    }
    return Status::OK();
}

} // namespace indexlibv2::index
