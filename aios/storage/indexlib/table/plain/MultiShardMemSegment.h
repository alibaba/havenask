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

#include "indexlib/framework/MemSegment.h"
#include "indexlib/index/common/ShardPartitioner.h"
#include "indexlib/table/plain/PlainMemSegment.h"

namespace indexlibv2::document {
class IDocument;
}

namespace indexlibv2::plain {

class MultiShardMemSegment : public framework::MemSegment
{
public:
    typedef std::function<std::shared_ptr<PlainMemSegment>(
        const config::TabletOptions*, const std::shared_ptr<config::ITabletSchema>&, const framework::SegmentMeta&)>
        PlainMemSegmentCreator;

    static std::shared_ptr<PlainMemSegment> CreatePlainMemSegment(const config::TabletOptions* options,
                                                                  const std::shared_ptr<config::ITabletSchema>& schema,
                                                                  const framework::SegmentMeta& segmentMeta);

public:
    MultiShardMemSegment(const std::shared_ptr<index::ShardPartitioner>& shardPartitioner, uint32_t levelNum,
                         const config::TabletOptions* options,
                         const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
                         const framework::SegmentMeta& segmentMeta,
                         const PlainMemSegmentCreator& memSegmentCreator = MultiShardMemSegment::CreatePlainMemSegment)
        : MemSegment(segmentMeta)
        , _shardPartitioner(shardPartitioner)
        , _levelNum(levelNum)
        , _options(options)
        , _tabletSchema(schema)
        , _memSegmentCreator(memSegmentCreator)
    {
        assert(_options);
    }
    ~MultiShardMemSegment() {}

public:
    Status Open(const framework::BuildResource& resource,
                indexlib::framework::SegmentMetrics* lastSegmentMetrics) override;

    Status Build(document::IDocumentBatch* batch) override;

    std::shared_ptr<indexlib::util::BuildResourceMetrics> GetBuildResourceMetrics() const override
    {
        return _buildResourceMetrics;
    }

    size_t EvaluateCurrentMemUsed() override;

    bool NeedDump() const override;
    std::pair<Status, std::vector<std::shared_ptr<framework::SegmentDumpItem>>> CreateSegmentDumpItems() override;
    void EndDump() override;
    void ValidateDocumentBatch(document::IDocumentBatch* batch) const override;
    void Seal() override;
    bool IsDirty() const override;
    void CollectSegmentDescription(const std::shared_ptr<framework::SegmentDescriptions>& segDescs) override;
    std::shared_ptr<framework::Segment> GetShardSegment(size_t shardIdx) const
    {
        if (shardIdx >= _shardSegments.size()) {
            return nullptr;
        }
        return _shardSegments[shardIdx];
    }

    uint32_t GetShardCount() const { return GetSegmentInfo()->GetShardCount(); }

public:
    void TEST_AddSegment(const std::shared_ptr<PlainMemSegment>& segment);
    PlainMemSegmentCreator TEST_GetPlainMemSegmentCreator() const { return _memSegmentCreator; }

private:
    void UpdateMemUse();
    indexlib::framework::SegmentMetrics* GetShardSegmentMetrics(uint32_t shardId,
                                                                indexlib::framework::SegmentMetrics* metrics);
    uint64_t ExtractKeyHash(const std::shared_ptr<document::IDocument>& doc);
    void UpdateSegmentInfo(document::IDocumentBatch* batch);
    Status OpenPlainMemSegment(const framework::BuildResource& resource,
                               indexlib::framework::SegmentMetrics* lastSegmentMetrics,
                               const std::shared_ptr<indexlib::file_system::Directory>& segmentDir, uint32_t shardId);

private:
    std::shared_ptr<index::ShardPartitioner> _shardPartitioner;
    uint32_t _levelNum = 1;
    const config::TabletOptions* _options = nullptr;
    std::vector<std::shared_ptr<PlainMemSegment>> _shardSegments;
    std::shared_ptr<indexlibv2::config::ITabletSchema> _tabletSchema;
    std::shared_ptr<indexlib::util::BuildResourceMetrics> _buildResourceMetrics;
    indexlib::util::BuildResourceMetricsNode* _metricsNode = nullptr;
    PlainMemSegmentCreator _memSegmentCreator;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::plain
