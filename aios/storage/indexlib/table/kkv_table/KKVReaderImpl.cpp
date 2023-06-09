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
#include "indexlib/table/kkv_table/KKVReaderImpl.h"

#include "autil/Log.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/Version.h"
#include "indexlib/index/common/field_format/pack_attribute/PackValueAdapter.h"
#include "indexlib/index/kkv/building/KKVMemIndexer.h"
#include "indexlib/index/kkv/built/KKVDiskIndexer.h"
#include "indexlib/table/plain/MultiShardDiskSegment.h"
#include "indexlib/table/plain/MultiShardMemSegment.h"

using namespace std;
using namespace autil;
using namespace indexlib;
using namespace indexlib::config;
using namespace indexlibv2::plain;
using namespace indexlibv2::framework;

namespace indexlibv2::table {

template <typename SKeyType>
Status KKVReaderImpl<SKeyType>::DoOpen(const std::shared_ptr<indexlibv2::config::KKVIndexConfig>& indexConfig,
                                       const framework::TabletData* tabletData) noexcept
{
    assert(_memShardReaders.empty());
    assert(_diskShardReaders.empty());

    _hasTTL = indexConfig->TTLEnabled();
    _skeyDictFieldType = indexConfig->GetSKeyDictKeyType();
    _pkeyHasherType = indexConfig->GetPrefixHashFunctionType();
    _skeyHasherType = indexConfig->GetSuffixHashFunctionType();

    auto status = LoadSegmentReader(indexConfig, tabletData);
    if (!status.IsOK()) {
        return status;
    }
    _hasSegReader = !_memShardReaders.empty() || !_diskShardReaders.empty();
    _shardCount = _memShardReaders.empty() ? _diskShardReaders.size() : _memShardReaders.size();
    // Align segment reader vector, Simplify 'if' judgment when Lookup
    AlignSegmentReaders(indexConfig);
    return Status::OK();
}

template <typename SKeyType>
Status
KKVReaderImpl<SKeyType>::LoadSegmentReader(const std::shared_ptr<indexlibv2::config::KKVIndexConfig>& indexConfig,
                                           const framework::TabletData* tabletData) noexcept
{
    if (0 == tabletData->GetSegmentCount()) {
        return Status::OK();
    }

    auto status = LoadMemSegmentReader(indexConfig, tabletData);
    if (!status.IsOK()) {
        return status;
    }

    return LoadDiskSegmentReader(indexConfig, tabletData);
}

template <typename SKeyType>
Status
KKVReaderImpl<SKeyType>::LoadMemSegmentReader(const std::shared_ptr<indexlibv2::config::KKVIndexConfig>& indexConfig,
                                              const framework::TabletData* tabletData) noexcept
{
    auto slice = tabletData->CreateSlice(framework::Segment::SegmentStatus::ST_BUILDING);
    auto status = AddMemSegments(indexConfig, slice);
    if (!status.IsOK()) {
        return status;
    }
    slice = tabletData->CreateSlice(framework::Segment::SegmentStatus::ST_DUMPING);
    return AddMemSegments(indexConfig, slice);
}

template <typename SKeyType>
Status KKVReaderImpl<SKeyType>::AddMemSegments(const std::shared_ptr<indexlibv2::config::KKVIndexConfig>& indexConfig,
                                               framework::TabletData::Slice& slice) noexcept
{
    for (auto it = slice.rbegin(); it != slice.rend(); it++) {
        auto segment = it->get();
        auto status = LoadMemSegmentReader(indexConfig, segment);
        if (!status.IsOK()) {
            return status;
        }
    }
    return Status::OK();
}

template <typename SKeyType>
Status
KKVReaderImpl<SKeyType>::LoadMemSegmentReader(const std::shared_ptr<indexlibv2::config::KKVIndexConfig>& indexConfig,
                                              Segment* segment) noexcept
{
    auto multiShardSegment = dynamic_cast<MultiShardMemSegment*>(segment);
    uint32_t shardCount = multiShardSegment->GetShardCount();
    if (_memShardReaders.size() == 0) {
        _memShardReaders.resize(shardCount);
    }
    if (_memShardReaders.size() != shardCount) {
        AUTIL_LOG(ERROR, "diff shard count in diff segments");
        return Status::InternalError();
    }

    assert(segment->GetSegmentSchema());
    schemaid_t segSchemaId = segment->GetSegmentSchema()->GetSchemaId();

    for (uint32_t shardIdx = 0; shardIdx < shardCount; shardIdx++) {
        auto shardSegment = multiShardSegment->GetShardSegment(shardIdx);
        assert(shardSegment->GetSegmentSchema());

        auto indexerPair = shardSegment->GetIndexer(indexConfig->GetIndexType(), indexConfig->GetIndexName());
        if (!indexerPair.first.IsOK()) {
            return Status::InternalError("no indexer for %s in segment[%d]", indexConfig->GetIndexName().c_str(),
                                         segment->GetSegmentId());
        }
        auto memIndexer = std::dynamic_pointer_cast<index::KKVMemIndexer<SKeyType>>(indexerPair.second);
        if (!memIndexer) {
            return Status::InternalError("not an instance of KKVMemIndexer %s in segment[%d]",
                                         indexConfig->GetIndexName().c_str(), segment->GetSegmentId());
        }
        auto memoryReader = memIndexer->CreateInMemoryReader();
        if (!memoryReader) {
            return Status::InternalError("failed to create memory reader for %s in segment[%d]",
                                         indexConfig->GetIndexName().c_str(), segment->GetSegmentId());
        }
        auto segmentLocator = std::make_shared<framework::Locator>(segment->GetSegmentInfo()->GetLocator());
        if (segSchemaId != GetReaderSchemaId()) {
            assert(false);
            AUTIL_LOG(ERROR,
                      "kkv not support multi schema now, mem segment [%d], "
                      "segmentSchemaId [%d], readerSchemaId [%d], shardIdx [%u]",
                      segment->GetSegmentId(), segSchemaId, GetReaderSchemaId(), shardIdx);
            return Status::InternalError("kkv not support multi schema now");
        }
        _memShardReaders[shardIdx].emplace_back(std::make_pair(std::move(memoryReader), segmentLocator));
    }

    return Status::OK();
}

template <typename SKeyType>
Status
KKVReaderImpl<SKeyType>::LoadDiskSegmentReader(const std::shared_ptr<indexlibv2::config::KVIndexConfig>& indexConfig,
                                               const framework::TabletData* tabletData)
{
    const auto& version = tabletData->GetOnDiskVersion();
    if (version.GetSegmentCount() == 0) {
        return Status::OK();
    }
    const auto segmentDescriptions = version.GetSegmentDescriptions();
    const auto& levelInfo = segmentDescriptions->GetLevelInfo();
    if (levelInfo->GetTopology() == framework::topo_sequence && levelInfo->GetLevelCount() > 1) {
        assert(false);
        auto status = Status::InternalError("kkv reader does not support sequence topology with more than one level");
        AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
        return status;
    }

    size_t shardCount = levelInfo->GetShardCount();
    if (_diskShardReaders.size() == 0) {
        _diskShardReaders.resize(shardCount);
    }
    if (_diskShardReaders.size() != shardCount) {
        AUTIL_LOG(ERROR, "diff shard count in diff segments");
        return Status::InternalError();
    }

    for (size_t shardIdx = 0; shardIdx < shardCount; ++shardIdx) {
        auto segIds = levelInfo->GetSegmentIds(shardIdx);
        // TODO(xinfei.sxf) BuiltKKVIterator loop from end to begin
        for (auto segIdsIter = segIds.rbegin(); segIdsIter != segIds.rend(); ++segIdsIter) {
            auto segment = tabletData->GetSegment(*segIdsIter);
            auto shardCount = segment->GetSegmentInfo()->shardCount;
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
            auto [status, indexer] = shardSegment->GetIndexer(indexConfig->GetIndexType(), indexConfig->GetIndexName());
            if (!status.IsOK()) {
                return Status::InternalError("no indexer for [%s] in segment[%d] ", indexConfig->GetIndexName().c_str(),
                                             shardSegment->GetSegmentId());
            }
            auto diskIndexer = std::dynamic_pointer_cast<index::KKVDiskIndexer<SKeyType>>(indexer);
            if (!diskIndexer) {
                return Status::InternalError("indexer for [%s] in segment[%d] with no DiskIndexer",
                                             indexConfig->GetIndexName().c_str(), shardSegment->GetSegmentId());
            }
            auto diskReader = diskIndexer->GetReader();
            assert(diskReader);
            auto segmentLocator = std::make_shared<framework::Locator>(segment->GetSegmentInfo()->GetLocator());
            assert(segment->GetSegmentSchema());

            schemaid_t segSchemaId = segment->GetSegmentSchema()->GetSchemaId();
            if (segSchemaId != GetReaderSchemaId()) {
                assert(false);
                AUTIL_LOG(ERROR,
                          "kkv not support multi schema now, mem segment [%d], "
                          "segmentSchemaId [%d], readerSchemaId [%d]",
                          segment->GetSegmentId(), segSchemaId, GetReaderSchemaId());
                return Status::InternalError("kkv not support multi schema now");
            }
            _diskShardReaders[shardIdx].push_back(std::make_pair(diskReader, segmentLocator));
        }
    }

    return Status::OK();
}

template <typename SKeyType>
void KKVReaderImpl<SKeyType>::AlignSegmentReaders(const std::shared_ptr<indexlibv2::config::KVIndexConfig>& indexConfig)
{
    // can both empty
    if (_memShardReaders.empty()) {
        _memShardReaders.resize(_diskShardReaders.size());
    }
    if (_diskShardReaders.empty()) {
        _diskShardReaders.resize(_memShardReaders.size());
    }
}

template <typename SKeyType>
typename KKVReaderImpl<SKeyType>::MemShardReaderVector& KKVReaderImpl<SKeyType>::TEST_GetMemoryShardReaders()
{
    return _memShardReaders;
}

template <typename SKeyType>
typename KKVReaderImpl<SKeyType>::DiskShardReaderVector& KKVReaderImpl<SKeyType>::TEST_GetDiskShardReaders()
{
    return _diskShardReaders;
}

EXPLICIT_DECLARE_ALL_SKEY_TYPE_TEMPLATE_CALSS(KKVReaderImpl);

} // namespace indexlibv2::table
