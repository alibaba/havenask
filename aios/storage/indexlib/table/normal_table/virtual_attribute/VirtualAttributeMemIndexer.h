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

#include "indexlib/base/Types.h"
#include "indexlib/index/IMemIndexer.h"
namespace indexlib::index {
template <typename DiskIndexerType, typename MemIndexerType>
class SingleAttributeBuilder;
}

namespace indexlibv2::table {

// VirtualAttributeMemIndexer does not inherit from AttributeMemIndexer directly because changing some methods in
// critical path to virtual will worsen performance.
class VirtualAttributeMemIndexer : public index::IMemIndexer
{
public:
    VirtualAttributeMemIndexer(const std::shared_ptr<index::IMemIndexer>& attrMemIndexer);
    ~VirtualAttributeMemIndexer();
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
    void UpdateMemUse(index::BuildingIndexMemoryUseUpdater* memUpdater) override;
    std::string GetIndexName() const override;
    autil::StringView GetIndexType() const override;
    void Seal() override;
    bool IsDirty() const override;
    std::shared_ptr<index::IMemIndexer> GetMemIndexer() const;

public:
    bool UpdateField(docid_t docId, const autil::StringView& attributeValue, bool isNull, const uint64_t* hashKey);

private:
    Status AddDocument(document::IDocument* doc);
    template <typename DiskIndexerType, typename MemIndexerType>
    friend class indexlib::index::SingleAttributeBuilder;

private:
    std::shared_ptr<index::IMemIndexer> _impl;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
