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

#include "autil/StringView.h"
#include "indexlib/index/IMemIndexer.h"
#include "indexlib/index/kv/KVIndexFields.h"
#include "indexlib/index/kv/MemoryUsage.h"
#include "indexlib/util/MemBuffer.h"

namespace autil::mem_pool {
class PoolBase;
}

namespace indexlibv2::config {
class KVIndexConfig;
}

namespace indexlibv2::document {
class KVDocument;
}

namespace indexlibv2::index {
struct SegmentStatistics;
class IKVSegmentReader;
class AttributeConvertor;
class PlainFormatEncoder;

class KVMemIndexerBase : public IMemIndexer
{
public:
    KVMemIndexerBase();
    virtual ~KVMemIndexerBase();

public:
    Status Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                document::extractor::IDocumentInfoExtractorFactory* docInfoExtractorFactory) override;
    Status Build(document::IDocumentBatch* docBatch) override;
    Status Build(const document::IIndexFields* indexFields, size_t n) override;
    Status Dump(autil::mem_pool::PoolBase* dumpPool,
                const std::shared_ptr<indexlib::file_system::Directory>& indexDirectory,
                const std::shared_ptr<framework::DumpParams>& dumpParams) override;
    void ValidateDocumentBatch(document::IDocumentBatch* docBatch) override;
    bool IsValidDocument(document::IDocument* doc) override;
    bool IsValidField(const document::IIndexFields* fields) override;
    void FillStatistics(const std::shared_ptr<indexlib::framework::SegmentMetrics>& segmentMetrics) override;
    void UpdateMemUse(BuildingIndexMemoryUseUpdater* memUpdater) override;
    std::string GetIndexName() const override;
    autil::StringView GetIndexType() const override;
    uint64_t GetIndexNameHash() const;
    void Seal() override;
    const std::shared_ptr<indexlibv2::config::KVIndexConfig>& GetIndexConfig() const { return _indexConfig; }
    bool IsDirty() const override { return true; }

public:
    virtual std::shared_ptr<IKVSegmentReader> CreateInMemoryReader() const = 0;

private:
    virtual Status DoInit() = 0;
    virtual bool IsFull() = 0;
    virtual Status Add(uint64_t key, const autil::StringView& value, uint32_t timestamp) = 0;
    virtual Status Delete(uint64_t key, uint32_t timestamp) = 0;
    virtual Status DoDump(autil::mem_pool::PoolBase* pool,
                          const std::shared_ptr<indexlib::file_system::Directory>& directory) = 0;
    virtual void UpdateMemoryUsage(MemoryUsage& memoryUsage) const = 0;
    virtual void DoFillStatistics(SegmentStatistics& stat) const = 0;

private:
    Status BuildSingleField(DocOperateType docType, const KVIndexFields::SingleField& singleField);
    Status AddField(const KVIndexFields::SingleField& field);
    Status DeleteField(const KVIndexFields::SingleField& field);

protected:
    std::shared_ptr<indexlibv2::config::KVIndexConfig> _indexConfig;
    uint64_t _indexNameHash;
    std::shared_ptr<AttributeConvertor> _attrConvertor;
    std::shared_ptr<PlainFormatEncoder> _plainFormatEncoder;
    indexlib::util::MemBuffer _memBuffer;
};

} // namespace indexlibv2::index
