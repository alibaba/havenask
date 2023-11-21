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
#include "indexlib/table/kv_table/KVReaderImpl.h"

#include "autil/EnvUtil.h"
#include "autil/Log.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/Version.h"
#include "indexlib/index/common/field_format/pack_attribute/PackValueAdapter.h"
#include "indexlib/index/kv/AdapterIgnoreFieldCalculator.h"
#include "indexlib/index/kv/AdapterKVSegmentReader.h"
#include "indexlib/index/kv/KVDiskIndexer.h"
#include "indexlib/index/kv/KVMemIndexerBase.h"
#include "indexlib/index/kv/KVSegmentReaderCreator.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/table/plain/MultiShardDiskSegment.h"
#include "indexlib/table/plain/MultiShardMemSegment.h"

using namespace std;
using namespace autil;
using namespace indexlib;
using namespace indexlib::config;
using namespace indexlibv2::plain;
using namespace indexlibv2::framework;

namespace indexlibv2::table {

AUTIL_LOG_SETUP(indexlib.table, KVReaderImpl);

Status KVReaderImpl::DoOpen(const std::shared_ptr<indexlibv2::config::KVIndexConfig>& kvIndexConfig,
                            const framework::TabletData* tabletData) noexcept
{
    assert(_memoryShardReaders.empty());
    assert(_diskShardReaders.empty());

    string envParam = autil::EnvUtil::getEnv("READ_REPORT_METRICS");
    if (envParam == "false") {
        _kvReportMetrics = false;
    }
    _hasTTL = kvIndexConfig->TTLEnabled();
    return LoadSegmentReader(kvIndexConfig, tabletData);
}

Status KVReaderImpl::LoadSegmentReader(const std::shared_ptr<indexlibv2::config::KVIndexConfig>& kvIndexConfig,
                                       const framework::TabletData* tabletData) noexcept
{
    if (0 == tabletData->GetSegmentCount()) {
        return Status::OK();
    }

    _ignoreFieldCalculator = std::make_shared<index::AdapterIgnoreFieldCalculator>();
    if (!_ignoreFieldCalculator->Init(kvIndexConfig, tabletData)) {
        return Status::InternalError("init AdapterIgnoreFieldCalculator fail for index [%s]",
                                     kvIndexConfig->GetIndexName().c_str());
    }

    auto status = LoadMemSegmentReader(kvIndexConfig, tabletData);
    if (!status.IsOK()) {
        return status;
    }

    const auto& version = tabletData->GetOnDiskVersion();
    if (version.GetSegmentCount() == 0) {
        return Status::OK();
    }
    const auto segmentDescriptions = version.GetSegmentDescriptions();
    const auto& levelInfo = segmentDescriptions->GetLevelInfo();
    if (levelInfo->GetTopology() == framework::topo_sequence && levelInfo->GetLevelCount() > 1) {
        assert(false);
        auto status = Status::InternalError("kv reader does not support sequence topology with more than one level");
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }
    return LoadDiskSegmentReader(kvIndexConfig, tabletData, levelInfo, levelInfo->GetShardCount());
}

Status KVReaderImpl::LoadMemSegmentReader(const std::shared_ptr<indexlibv2::config::KVIndexConfig>& kvIndexConfig,
                                          const framework::TabletData* tabletData) noexcept
{
    auto slice = tabletData->CreateSlice(framework::Segment::SegmentStatus::ST_BUILDING);
    auto status = AddMemSegments(kvIndexConfig, slice);
    if (!status.IsOK()) {
        return status;
    }
    slice = tabletData->CreateSlice(framework::Segment::SegmentStatus::ST_DUMPING);
    return AddMemSegments(kvIndexConfig, slice);
}

Status KVReaderImpl::AddMemSegments(const std::shared_ptr<indexlibv2::config::KVIndexConfig>& kvIndexConfig,
                                    framework::TabletData::Slice& slice) noexcept
{
    for (auto it = slice.rbegin(); it != slice.rend(); it++) {
        auto segment = it->get();
        auto status = LoadMemSegmentReader(kvIndexConfig, segment);
        if (!status.IsOK()) {
            return status;
        }
    }
    return Status::OK();
}

Status KVReaderImpl::LoadMemSegmentReader(const std::shared_ptr<indexlibv2::config::KVIndexConfig>& kvIndexConfig,
                                          Segment* segment) noexcept
{
    auto multiShardSegment = dynamic_cast<MultiShardMemSegment*>(segment);
    uint32_t shardCount = multiShardSegment->GetShardCount();
    if (_memoryShardReaders.size() == 0) {
        _memoryShardReaders.resize(shardCount);
    }
    if (_memoryShardReaders.size() != shardCount) {
        AUTIL_LOG(ERROR, "diff shard count in diff segments");
        return Status::InternalError();
    }

    assert(segment->GetSegmentSchema());
    schemaid_t segSchemaId = segment->GetSegmentSchema()->GetSchemaId();
    auto ignoreFields = _ignoreFieldCalculator->GetIgnoreFields(std::min(segSchemaId, _readerSchemaId),
                                                                std::max(segSchemaId, _readerSchemaId));
    for (uint32_t shardIdx = 0; shardIdx < shardCount; shardIdx++) {
        auto shardSegment = multiShardSegment->GetShardSegment(shardIdx);
        assert(shardSegment->GetSegmentSchema());
        auto indexerPair = shardSegment->GetIndexer(kvIndexConfig->GetIndexType(), kvIndexConfig->GetIndexName());
        if (!indexerPair.first.IsOK()) {
            return Status::InternalError("no indexer for %s in segment[%d]", kvIndexConfig->GetIndexName().c_str(),
                                         segment->GetSegmentId());
        }
        auto memIndexer = std::dynamic_pointer_cast<index::KVMemIndexerBase>(indexerPair.second);
        if (!memIndexer) {
            return Status::InternalError("not an instance of KVMemIndexerBase %s in segment[%d]",
                                         kvIndexConfig->GetIndexName().c_str(), segment->GetSegmentId());
        }
        auto memoryReader = memIndexer->CreateInMemoryReader();
        if (!memoryReader) {
            return Status::InternalError("failed to create memory reader for %s in segment[%d]",
                                         kvIndexConfig->GetIndexName().c_str(), segment->GetSegmentId());
        }
        auto segmentLocator = std::make_shared<framework::Locator>(segment->GetSegmentInfo()->GetLocator());
        if (segSchemaId != _readerSchemaId) {
            AUTIL_LOG(INFO,
                      "Begin Load AdapterSegmentReader for mem segment [%d], "
                      "segmentSchemaId [%d], readerSchemaId [%d], shardIdx [%u]",
                      segment->GetSegmentId(), segSchemaId, _readerSchemaId, shardIdx);
        }
        auto segmentReaderCreator = std::make_shared<index::KVSegmentReaderCreator>();
        auto [status2, segmentReader] = segmentReaderCreator->CreateSegmentReader(
            memoryReader, memIndexer->GetIndexConfig(), kvIndexConfig, ignoreFields, true);
        RETURN_IF_STATUS_ERROR(status2, "failed to create memory adapter segment reader for %s in segment[%d]",
                               kvIndexConfig->GetIndexName().c_str(), segment->GetSegmentId());
        _memoryShardReaders[shardIdx].emplace_back(std::make_pair(std::move(segmentReader), segmentLocator));
    }

    return Status::OK();
}

Status KVReaderImpl::LoadDiskSegmentReader(const std::shared_ptr<indexlibv2::config::KVIndexConfig>& kvIndexConfig,
                                           const framework::TabletData* tabletData,
                                           const std::shared_ptr<framework::LevelInfo>& levelInfo,
                                           size_t shardCount) noexcept
{
    if (_diskShardReaders.size() == 0) {
        _diskShardReaders.resize(shardCount);
    }
    if (_diskShardReaders.size() != shardCount) {
        AUTIL_LOG(ERROR, "diff shard count in diff segments");
        return Status::InternalError();
    }
    for (size_t shardIdx = 0; shardIdx < shardCount; ++shardIdx) {
        auto segIds = levelInfo->GetSegmentIds(shardIdx);
        for (size_t segIdx = 0; segIdx < segIds.size(); ++segIdx) {
            auto segment = tabletData->GetSegment(segIds[segIdx]);
            auto shardCount = segment->GetSegmentInfo()->shardCount;
            auto shardId = segment->GetSegmentInfo()->shardId;
            auto isMergedSegment = segment->GetSegmentInfo()->IsMergedSegment();
            uint32_t levelIdx = 0;
            uint32_t inLevelIdx = 0;
            if (levelInfo->FindPosition(segIds[segIdx], levelIdx, inLevelIdx)) {
                // bulkload segment appear in level 0 maybe single shard segment,
                // exclude unmatched shard id bulkload segment in level 0
                if (shardId != shardIdx && levelIdx == 0 && isMergedSegment) {
                    continue;
                }
            }
            auto multiShardDiskSegment = std::dynamic_pointer_cast<MultiShardDiskSegment>(segment);
            SegmentPtr shardSegment;
            if (1 == shardCount) {
                shardSegment = multiShardDiskSegment->GetShardSegment(0);
            } else {
                assert(shardIdx < shardCount);
                shardSegment = multiShardDiskSegment->GetShardSegment(shardIdx);
            }
            if (shardSegment == nullptr) {
                return Status::Corruption("shard[%lu] segment is invalid in multi shard disk segment[%d]", shardIdx,
                                          multiShardDiskSegment->GetSegmentId());
            }
            if (shardSegment->GetSegmentInfo()->docCount == 0) {
                continue; // there is no document in current segment, so do nothing
            }
            auto [status, indexer] =
                shardSegment->GetIndexer(kvIndexConfig->GetIndexType(), kvIndexConfig->GetIndexName());
            if (!status.IsOK()) {
                return Status::InternalError("no indexer for [%s] in segment[%d] ",
                                             kvIndexConfig->GetIndexName().c_str(), shardSegment->GetSegmentId());
            }
            auto diskIndexer = std::dynamic_pointer_cast<index::KVDiskIndexer>(indexer);
            if (!diskIndexer) {
                return Status::InternalError("indexer for [%s] in segment[%d] with no OnDiskIndexer",
                                             kvIndexConfig->GetIndexName().c_str(), shardSegment->GetSegmentId());
            }
            auto segmentLocator = std::make_shared<framework::Locator>(segment->GetSegmentInfo()->GetLocator());
            assert(segment->GetSegmentSchema());
            schemaid_t segSchemaId = segment->GetSegmentSchema()->GetSchemaId();
            auto ignoreFields = _ignoreFieldCalculator->GetIgnoreFields(std::min(segSchemaId, _readerSchemaId),
                                                                        std::max(segSchemaId, _readerSchemaId));
            if (segSchemaId != _readerSchemaId) {
                AUTIL_LOG(INFO,
                          "Begin Load AdapterSegmentReader for disk segment [%d], "
                          "segmentSchemaId [%d], readerSchemaId [%d]",
                          segment->GetSegmentId(), segSchemaId, _readerSchemaId);
            }
            auto segmentReaderCreator = std::make_shared<index::KVSegmentReaderCreator>();
            auto [status2, segmentReader] = segmentReaderCreator->CreateSegmentReader(
                diskIndexer->GetReader(), diskIndexer->GetIndexConfig(), kvIndexConfig, ignoreFields, true);
            RETURN_IF_STATUS_ERROR(status2, "failed to create adapter segment reader for [%s] in segment[%d]]",
                                   kvIndexConfig->GetIndexName().c_str(), segment->GetSegmentId());
            _diskShardReaders[shardIdx].push_back(std::make_pair(std::move(segmentReader), segmentLocator));
        }
    }

    return Status::OK();
}

KVReaderImpl::SegmentShardReaderVector& KVReaderImpl::TEST_GetMemoryShardReaders() { return _memoryShardReaders; }
KVReaderImpl::SegmentShardReaderVector& KVReaderImpl::TEST_GetDiskShardReaders() { return _diskShardReaders; }

} // namespace indexlibv2::table
