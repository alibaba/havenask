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

#include "indexlib/config/ITabletSchema.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/index/IIndexer.h"

namespace indexlibv2::framework {

class FakeSegment final : public Segment
{
public:
    FakeSegment(SegmentStatus status = Segment::SegmentStatus::ST_BUILT);
    FakeSegment(segmentid_t segId, SegmentStatus status = Segment::SegmentStatus::ST_BUILT,
                Locator locator = Locator());
    FakeSegment(segmentid_t segId, const std::shared_ptr<config::ITabletSchema>& schema,
                SegmentStatus status = Segment::SegmentStatus::ST_BUILT);
    ~FakeSegment();

public:
    void SetSegmentId(segmentid_t segId);
    void SetSegmentSchema(const std::shared_ptr<config::ITabletSchema>& schema);

    std::pair<Status, std::shared_ptr<index::IIndexer>> GetIndexer(const std::string& type,
                                                                   const std::string& indexName) override;

    void AddIndexer(const std::string& type, const std::string& indexName,
                    std::shared_ptr<index::IIndexer> indexer) override;

    void DeleteIndexer(const std::string& type, const std::string& indexName) override;

    size_t EstimateMemUsed(const std::shared_ptr<config::ITabletSchema>& schema) override { return 0; }
    size_t EvaluateCurrentMemUsed() override { return 0; }

public:
    using Key = std::pair<std::string, std::string>;
    std::map<Key, std::shared_ptr<index::IIndexer>> _indexers;
};

} // namespace indexlibv2::framework
