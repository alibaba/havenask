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

#include <memory>

#include "autil/Log.h"
#include "autil/mem_pool/pool_allocator.h"
#include "indexlib/document/normal/IndexDocument.h"
#include "indexlib/index/IMemIndexer.h"
#include "indexlib/index/statistics_term/StatisticsTermIndexConfig.h"
#include "indexlib/util/SimplePool.h"

namespace indexlib::file_system {
class IDirectory;
class FileWriter;
} // namespace indexlib::file_system

namespace indexlibv2::index {

class StatisticsTermMemIndexer : public IMemIndexer
{
public:
    explicit StatisticsTermMemIndexer(std::map<std::string, size_t>& initialKeyCount);
    ~StatisticsTermMemIndexer() = default;

public:
    Status Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                document::extractor::IDocumentInfoExtractorFactory* docInfoExtractorFactory) override;
    Status Build(document::IDocumentBatch* docBatch) override;
    Status Build(const document::IIndexFields* indexFields, size_t n) override;
    Status Dump(autil::mem_pool::PoolBase* dumpPool,
                const std::shared_ptr<indexlib::file_system::Directory>& indexDirectory,
                const std::shared_ptr<framework::DumpParams>& params) override;
    void ValidateDocumentBatch(document::IDocumentBatch* docBatch) override;
    bool IsValidDocument(document::IDocument* doc) override;
    bool IsValidField(const document::IIndexFields* fields) override;
    void FillStatistics(const std::shared_ptr<indexlib::framework::SegmentMetrics>& segmentMetrics) override;
    void UpdateMemUse(BuildingIndexMemoryUseUpdater* memUpdater) override;
    std::string GetIndexName() const override;
    autil::StringView GetIndexType() const override;
    void Seal() override;
    bool IsDirty() const override;

public:
    static std::string GetStatisticsTermKey(const std::string& indexName, const std::string& invertedIndexName);

private:
    Status AddDocument(const std::shared_ptr<indexlib::document::IndexDocument>& indexDoc);
    std::pair<Status, std::shared_ptr<indexlib::file_system::FileWriter>>
    CreateFileWriter(const std::shared_ptr<indexlib::file_system::IDirectory>& directory,
                     const std::string& fileName) const;

private:
    using TermAllocator = autil::mem_pool::pool_allocator<std::pair<const size_t, std::string>>;
    using TermHashMap =
        std::unordered_map<size_t, std::string, std::hash<size_t>, std::equal_to<size_t>, TermAllocator>;

    const std::map<std::string, size_t> _initialKeyCount;
    indexlib::util::SimplePool _simplePool;
    std::shared_ptr<TermAllocator> _allocator;
    std::shared_ptr<StatisticsTermIndexConfig> _indexConfig;
    std::map<std::string, std::unique_ptr<TermHashMap>> _termOriginValueMap;
    std::map<std::string, std::pair<size_t, size_t>> _termMetas; // index1 -> <offset, blockLen>

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
