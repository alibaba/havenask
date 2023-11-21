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

#include "indexlib/framework/DiskSegment.h"
#include "indexlib/table/plain/PlainDiskSegment.h"

namespace indexlibv2::plain {

class MultiShardDiskSegment : public framework::DiskSegment
{
public:
    MultiShardDiskSegment(const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
                          const framework::SegmentMeta& segmentMeta, const framework::BuildResource& buildResource)
        : DiskSegment(segmentMeta)
        , _buildResource(buildResource)
        , _tabletSchema(schema)
    {
    }
    ~MultiShardDiskSegment() {}

    Status Open(const std::shared_ptr<MemoryQuotaController>& memoryQuotaController, OpenMode mode) override;
    Status Reopen(const std::vector<std::shared_ptr<config::ITabletSchema>>& schema) override
    {
        assert(false);
        return Status::Corruption();
    }
    std::pair<Status, size_t> EstimateMemUsed(const std::shared_ptr<config::ITabletSchema>& schema) override;
    size_t EvaluateCurrentMemUsed() override;
    void CollectSegmentDescription(const std::shared_ptr<framework::SegmentDescriptions>& segDescs) override;
    std::shared_ptr<framework::Segment> GetShardSegment(size_t shardIdx) const;
    uint32_t GetShardCount() const { return GetSegmentInfo()->GetShardCount(); }

public:
    void TEST_AddSegment(const std::shared_ptr<PlainDiskSegment>& segment);

private:
    Status OpenBuildSegment(const std::shared_ptr<MemoryQuotaController>& memoryQuotaController, OpenMode mode);
    Status OpenSingleShardSegment(uint32_t shardId, const std::shared_ptr<MemoryQuotaController>& memoryQuotaController,
                                  OpenMode mode);

private:
    std::vector<std::shared_ptr<PlainDiskSegment>> _shardSegments;
    framework::BuildResource _buildResource;
    std::shared_ptr<indexlibv2::config::ITabletSchema> _tabletSchema;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::plain
