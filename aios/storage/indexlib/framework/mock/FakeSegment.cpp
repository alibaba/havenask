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
#include "indexlib/framework/mock/FakeSegment.h"

#include "indexlib/framework/SegmentInfo.h"

namespace indexlibv2::framework {

FakeSegment::FakeSegment(SegmentStatus status) : FakeSegment(INVALID_SEGMENTID, nullptr, status) {}

FakeSegment::FakeSegment(segmentid_t segId, const std::shared_ptr<config::ITabletSchema>& schema, SegmentStatus status)
    : Segment(status)
{
    _segmentMeta.segmentId = segId;
    _segmentMeta.schema = schema;
    _segmentMeta.segmentInfo = std::make_shared<SegmentInfo>();
}
FakeSegment::FakeSegment(segmentid_t segId, SegmentStatus status, Locator locator) : FakeSegment(segId, nullptr, status)
{
    _segmentMeta.segmentInfo->SetLocator(locator);
}

FakeSegment::~FakeSegment() = default;

void FakeSegment::SetSegmentId(segmentid_t segId) { _segmentMeta.segmentId = segId; }

void FakeSegment::SetSegmentSchema(const std::shared_ptr<config::ITabletSchema>& schema)
{
    _segmentMeta.schema = schema;
}

std::pair<Status, std::shared_ptr<index::IIndexer>> FakeSegment::GetIndexer(const std::string& type,
                                                                            const std::string& indexName)
{
    auto it = _indexers.find({type, indexName});
    if (it == _indexers.end()) {
        return {Status::NotFound(), nullptr};
    }
    return {Status::OK(), it->second};
}

void FakeSegment::AddIndexer(const std::string& type, const std::string& indexName,
                             std::shared_ptr<index::IIndexer> indexer)
{
    _indexers.emplace(std::make_pair(type, indexName), std::move(indexer));
}

void FakeSegment::DeleteIndexer(const std::string& type, const std::string& indexName)
{
    _indexers.erase({type, indexName});
}

} // namespace indexlibv2::framework
