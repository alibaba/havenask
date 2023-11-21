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
#include "indexlib/framework/MemSegment.h"
#include "indexlib/framework/SegmentMeta.h"

namespace indexlibv2::index {
class IMemIndexer;
}
namespace indexlibv2::framework {

class FakeMemSegment final : public MemSegment
{
public:
    explicit FakeMemSegment(const SegmentMeta& segmentMeta);
    ~FakeMemSegment();

public:
    Status Open(const BuildResource& resource, indexlib::framework::SegmentMetrics* lastSegmentMetrics) override;
    Status Build(document::IDocumentBatch* batch) override;
    std::shared_ptr<indexlib::util::BuildResourceMetrics> GetBuildResourceMetrics() const override;
    bool NeedDump() const override;
    std::pair<Status, std::vector<std::shared_ptr<SegmentDumpItem>>> CreateSegmentDumpItems() override;
    void EndDump() override;
    void Seal() override;
    bool IsDirty() const override;
    std::pair<Status, std::shared_ptr<index::IIndexer>> GetIndexer(const std::string& type,
                                                                   const std::string& indexName) override;
    void AddIndexer(const std::string& type, const std::string& indexName,
                    std::shared_ptr<index::IIndexer> indexer) override;
    void DeleteIndexer(const std::string& type, const std::string& indexName) override;
    std::pair<Status, size_t> EstimateMemUsed(const std::shared_ptr<config::ITabletSchema>& schema) override;
    size_t EvaluateCurrentMemUsed() override;
    void ValidateDocumentBatch(document::IDocumentBatch* batch) const override;

public:
    Status Dump();
    void SetSegmentId(segmentid_t segId);
    void SetSegmentSchema(const std::shared_ptr<config::ITabletSchema>& schema);

public:
    // <type, name> --> IIndexer
    std::map<std::pair<std::string, std::string>, std::shared_ptr<index::IMemIndexer>> _indexers;
    std::shared_ptr<indexlib::util::BuildResourceMetrics> _buildResourceMetrics;
    bool _isDirty = false;
};

} // namespace indexlibv2::framework
