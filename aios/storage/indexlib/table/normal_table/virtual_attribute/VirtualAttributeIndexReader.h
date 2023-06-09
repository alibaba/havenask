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
#include "autil/Log.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/IIndexReader.h"
#include "indexlib/index/attribute/AttributeIteratorTyped.h"
#include "indexlib/index/attribute/SingleValueAttributeDiskIndexer.h"
#include "indexlib/index/attribute/SingleValueAttributeMemIndexer.h"
#include "indexlib/table/normal_table/virtual_attribute/VirtualAttributeDiskIndexer.h"
#include "indexlib/table/normal_table/virtual_attribute/VirtualAttributeMemIndexer.h"

namespace indexlibv2::table {
template <typename T>
class VirtualAttributeIndexReader : public index::IIndexReader
{
public:
    VirtualAttributeIndexReader() : IIndexReader(), _baseDocId(INVALID_DOCID), _iterator(nullptr) {}
    ~VirtualAttributeIndexReader() {}

public:
    Status Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                const framework::TabletData* tabletData) override;
    bool Seek(docid_t docId, T& value);

private:
    Status InitBuildingAttributeReader(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                       docid_t buildingBaseDocIdWithoutMergedSegment,
                                       const std::vector<framework::Segment*>& buildingSegments);

private:
    std::vector<std::shared_ptr<index::SingleValueAttributeDiskIndexer<T>>> _onDiskIndexers;
    std::vector<std::pair<docid_t, std::shared_ptr<index::SingleValueAttributeMemReader<T>>>> _memReaders;
    std::vector<uint64_t> _segmentDocCount;
    docid_t _baseDocId;
    std::unique_ptr<index::AttributeIteratorTyped<T>> _iterator;
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, VirtualAttributeIndexReader, T);

template <typename T>
inline Status VirtualAttributeIndexReader<T>::Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                                   const framework::TabletData* tabletData)
{
    auto slice = tabletData->CreateSlice();
    std::vector<framework::Segment*> buildingSegments;
    docid_t buildingBaseDocIdWithoutMergedSegment = 0;
    _baseDocId = 0;
    for (auto it = slice.begin(); it != slice.end(); it++) {
        framework::Segment* segment = it->get();
        auto status = segment->GetSegmentStatus();
        size_t docCount = segment->GetSegmentInfo()->docCount;
        if (status == framework::Segment::SegmentStatus::ST_BUILT && docCount == 0) {
            continue; // there is no document in current segment, so do nothing
        }
        auto indexerPair = segment->GetIndexer(indexConfig->GetIndexType(), indexConfig->GetIndexName());
        if (!indexerPair.first.IsOK()) {
            if (indexerPair.first.IsNotFound()) {
                _baseDocId += docCount;
                continue;
            } else {
                auto status = Status::InternalError("no indexer for [%s] in segment [%d]",
                                                    indexConfig->GetIndexName().c_str(), segment->GetSegmentId());
                AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
                return status;
            }
        }
        auto indexer = indexerPair.second;
        if (framework::Segment::SegmentStatus::ST_BUILT == status) {
            auto diskIndexerWrapper = std::dynamic_pointer_cast<VirtualAttributeDiskIndexer>(indexer);
            auto diskIndexer = std::dynamic_pointer_cast<index::SingleValueAttributeDiskIndexer<T>>(
                diskIndexerWrapper->GetDiskIndexer());
            if (!diskIndexer) {
                auto status = Status::InternalError("indexer for [%s] in segment [%d] with no ondiskindexer",
                                                    indexConfig->GetIndexName().c_str(), segment->GetSegmentId());
                AUTIL_LOG(ERROR, "%s", status.ToString().c_str());
                return status;
            }
            _onDiskIndexers.emplace_back(diskIndexer);
            _segmentDocCount.emplace_back(docCount);
            buildingBaseDocIdWithoutMergedSegment += docCount;
        } else if (framework::Segment::SegmentStatus::ST_DUMPING == status ||
                   framework::Segment::SegmentStatus::ST_BUILDING == status) {
            buildingSegments.emplace_back(segment);
        }
    }
    auto st = InitBuildingAttributeReader(indexConfig, buildingBaseDocIdWithoutMergedSegment, buildingSegments);
    if (!st.IsOK()) {
        AUTIL_LOG(ERROR, "init building attr reader failed, error[%s]", st.ToString().c_str());
        return st;
    }
    _iterator.reset(new index::AttributeIteratorTyped<T>(_onDiskIndexers, _memReaders, nullptr, _segmentDocCount,
                                                         nullptr, nullptr));
    return Status::OK();
}

template <typename T>
inline Status
VirtualAttributeIndexReader<T>::InitBuildingAttributeReader(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                                            docid_t buildingBaseDocIdWithoutMergedSegment,
                                                            const std::vector<framework::Segment*>& buildingSegments)
{
    docid_t baseDocId = buildingBaseDocIdWithoutMergedSegment;
    for (auto segment : buildingSegments) {
        auto indexerPair = segment->GetIndexer(indexConfig->GetIndexType(), indexConfig->GetIndexName());
        if (!indexerPair.first.IsOK()) {
            AUTIL_LOG(ERROR, "get indexer failed");
            return Status::InternalError("get indexer failed");
        }
        auto indexer = indexerPair.second;
        auto indexerWrapper = std::dynamic_pointer_cast<VirtualAttributeMemIndexer>(indexer);
        auto memIndexer =
            std::dynamic_pointer_cast<index::SingleValueAttributeMemIndexer<T>>(indexerWrapper->GetMemIndexer());
        if (memIndexer) {
            auto memReader =
                std::dynamic_pointer_cast<index::SingleValueAttributeMemReader<T>>(memIndexer->CreateInMemReader());
            assert(memReader);
            _memReaders.emplace_back(std::make_pair(baseDocId, memReader));
            baseDocId += segment->GetSegmentInfo()->docCount;
        } else {
            AUTIL_LOG(ERROR, "get indexer failed");
            return Status::InternalError("get indexer failed");
        }
    }
    return Status::OK();
}

template <typename T>
inline bool VirtualAttributeIndexReader<T>::Seek(docid_t docId, T& value)
{
    if (docId < _baseDocId) {
        assert(false);
        return false;
    }
    return _iterator->Seek(docId - _baseDocId, value);
}

} // namespace indexlibv2::table
