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

#include <map>
#include <memory>

#include "autil/Log.h"
#include "indexlib/base/MemoryQuotaController.h"
#include "indexlib/base/Status.h"
#include "indexlib/config/SortDescription.h"
#include "indexlib/document/extractor/plain/DocumentInfoExtractorFactory.h"
#include "indexlib/framework/MemSegment.h"
#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/index/IIndexFactory.h"
#include "indexlib/index/IndexerParameter.h"

namespace indexlib::config {
// class IndexPartitionOptions;
class IndexPartitionSchema;
} // namespace indexlib::config

namespace indexlib::framework {
class SegmentMetrics;
}
namespace indexlib::util {
class BuildResourceMetrics;
class BuildResourceMetricsNode;
} // namespace indexlib::util

namespace indexlibv2 {
class MemoryQuotaController;
}

namespace indexlibv2::config {
class TabletOptions;
class ITabletSchema;
class IIndexConfig;
} // namespace indexlibv2::config

namespace indexlibv2::document {
class IDocumentBatch;
class PlainDocument;
template <typename DocumentType>
class TemplateDocumentBatch;
namespace extractor {
class IDocumentInfoExtractorFactory;
}
} // namespace indexlibv2::document

namespace indexlibv2::framework {
struct BuildResource;
struct DumpParams;
} // namespace indexlibv2::framework

namespace indexlibv2::index {
class IMemIndexer;
class IIndexer;
class BuildingIndexMemoryUseUpdater;
} // namespace indexlibv2::index

namespace indexlib::document {
class NormalDocument;
}

namespace indexlibv2::plain {

class PlainDocumentBatch;

class PlainMemSegment : public framework::MemSegment
{
public:
    using IndexMapKey = std::pair<std::string, std::string>;
    using IndexerAndMemUpdaterPair =
        std::pair<std::shared_ptr<index::IMemIndexer>, std::shared_ptr<index::BuildingIndexMemoryUseUpdater>>;
    struct IndexerResource {
        std::shared_ptr<config::IIndexConfig> indexConfig;
        index::IIndexFactory* indexerFactory = nullptr;
        index::IndexerParameter indexerParam;
        size_t lastMemUseSize = 0;
    };

    PlainMemSegment(const config::TabletOptions* options, const std::shared_ptr<config::ITabletSchema>& schema,
                    const framework::SegmentMeta& segmentMeta)
        : framework::MemSegment(segmentMeta)
        , _options(options)
        , _schema(schema)
        , _isDirty(false)
    {
        assert(_options);
    }
    ~PlainMemSegment() = default;

    Status Build(document::IDocumentBatch* batch) override;
    Status Open(const framework::BuildResource& resource,
                indexlib::framework::SegmentMetrics* lastSegmentMetrics) override;
    std::pair<Status, std::shared_ptr<index::IIndexer>> GetIndexer(const std::string& type,
                                                                   const std::string& indexName) override;
    std::shared_ptr<indexlib::util::BuildResourceMetrics> GetBuildResourceMetrics() const override
    {
        return _buildResourceMetrics;
    }
    bool NeedDump() const override;
    void Seal() override;
    std::pair<Status, std::vector<std::shared_ptr<framework::SegmentDumpItem>>> CreateSegmentDumpItems() override;
    void EndDump() override { UpdateMemUse(); }
    void ValidateDocumentBatch(document::IDocumentBatch* batch);
    size_t EvaluateCurrentMemUsed() override { return _segmentMemController->GetAllocatedQuota(); }
    bool IsDirty() const override { return _isDirty; }

public:
    void PostBuildActions(const indexlibv2::framework::Locator& locator, int64_t maxTimestamp, int64_t maxTTL,
                          uint64_t addDocCount);

public:
    void TEST_AddIndexer(const std::string& type, const std::string& indexName,
                         const std::shared_ptr<indexlibv2::index::IIndexer> indexer);

protected:
    virtual std::pair<Status, std::shared_ptr<framework::DumpParams>> CreateDumpParams();
    virtual void CalcMemCostInCreateDumpParams() {}
    virtual const std::vector<std::shared_ptr<indexlibv2::index::IMemIndexer>>& GetBuildIndexers();
    virtual std::pair<Status, size_t> GetLastSegmentMemUse(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                                           const indexlib::framework::SegmentMetrics* metrics) const
    {
        return std::make_pair(Status::NotFound(), 0);
    }
    virtual void PrepareIndexerParameter(const framework::BuildResource& resource, int64_t maxMemoryUseInBytes,
                                         index::IndexerParameter& indexerParam) const;

private:
    Status PrepareIndexers(const framework::BuildResource& resource);
    void UpdateMemUse();
    void Cleanup() { _indexMap.clear(); }

    IndexMapKey GenerateIndexMapKey(const std::string& indexType, const std::string& indexName) const
    {
        IndexMapKey indexMapKey = std::make_pair(indexType, indexName);
        return indexMapKey;
    }
    void UpdateSegmentInfo(const indexlibv2::framework::Locator& locator, int64_t maxTimestamp, int64_t maxTTL,
                           uint64_t addDocCount);
    Status GenerateIndexerResource(const framework::BuildResource& resource,
                                   std::vector<IndexerResource>& indexerResources);
    Status BuildPlainDoc(document::TemplateDocumentBatch<document::PlainDocument>* plainDocBatch);
    void ValidateDocumentBatch(document::TemplateDocumentBatch<document::PlainDocument>* plainDocBatch);

protected:
    const config::TabletOptions* _options = nullptr;
    std::shared_ptr<config::ITabletSchema> _schema;
    std::shared_ptr<indexlib::util::BuildResourceMetrics> _buildResourceMetrics;
    config::SortDescriptions _sortDescriptions;
    bool _isDirty = false;
    std::map<IndexMapKey, IndexerAndMemUpdaterPair> _indexMap;
    // _indexMapForValidation is an optimization to be used in ValidateDocumentBatch.
    // Currently all attribute indexers validate documents in the same way, so we can validate attribute indexers only
    // once. The same goes for inverted indexers. This becomes more important when we have a lot of indexers like
    // mainse.
    std::map<IndexMapKey, IndexerAndMemUpdaterPair> _indexMapForValidation;
    std::vector<std::shared_ptr<indexlibv2::index::IMemIndexer>> _indexers;
    std::shared_ptr<indexlib::framework::SegmentMetrics> _lastSegmentMetrics;

private:
    std::unique_ptr<MemoryQuotaController> _segmentMemController;
    std::unique_ptr<document::extractor::IDocumentInfoExtractorFactory> _docInfoExtractorFactory;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::plain
