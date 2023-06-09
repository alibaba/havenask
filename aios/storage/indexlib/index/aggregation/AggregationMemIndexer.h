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
#include "indexlib/index/IMemIndexer.h"
#include "indexlib/index/aggregation/AggregationIndexConfig.h"
#include "indexlib/index/aggregation/AggregationIndexFields.h"

namespace indexlib::file_system {
class IDirectory;
class FileWriter;
} // namespace indexlib::file_system

namespace indexlibv2::index {
class AttributeConvertor;
class ISegmentReader;
struct ReadWriteState;

class AggregationMemIndexer : public IMemIndexer
{
public:
    explicit AggregationMemIndexer(size_t initialKeyCount = 0);
    ~AggregationMemIndexer();

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
    std::shared_ptr<ISegmentReader> CreateInMemoryReader() const;
    std::shared_ptr<ISegmentReader> CreateInMemoryDeleteReader() const;

private:
    Status InitValueDecoder();
    Status BuildSingleField(const AggregationIndexFields::SingleField& singleField);

private:
    size_t _initialKeyCount = 0;
    std::shared_ptr<config::AggregationIndexConfig> _indexConfig;
    std::shared_ptr<AttributeConvertor> _valueDecoder;
    std::shared_ptr<ReadWriteState> _state; // shared state between write and read
    size_t _indexHash = 0;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
