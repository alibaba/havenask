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

#include "autil/Log.h"
#include "indexlib/config/IIndexConfig.h"
#include "indexlib/document/extractor/IDocumentInfoExtractor.h"
#include "indexlib/document/extractor/IDocumentInfoExtractorFactory.h"
#include "indexlib/index/IMemIndexer.h"
#include "indexlib/index/MemIndexerParameter.h"
#include "indexlib/index/attribute/AttributeMemReader.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertor.h"
#include "indexlib/util/MMapAllocator.h"
#include "indexlib/util/SimplePool.h"
#include "indexlib/util/counter/AccumulativeCounter.h"

namespace indexlib::index {
template <typename DiskIndexerType, typename MemIndexerType>
class SingleAttributeBuilder;
}

namespace indexlibv2::index {
class AttributeDiskIndexer;

class AttributeMemIndexer : public IMemIndexer
{
public:
    // move counter and isOffline and flushRtIndex to IndexMeta?
    AttributeMemIndexer(const MemIndexerParameter& indexerParam) : _indexerParam(indexerParam) {}
    ~AttributeMemIndexer() = default;

public:
    Status Init(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                document::extractor::IDocumentInfoExtractorFactory* docInfoExtractorFactory) override;
    Status Build(document::IDocumentBatch* docBatch) override;
    Status Build(const document::IIndexFields* indexFields, size_t n) override;
    void ValidateDocumentBatch(document::IDocumentBatch* docBatch) override;
    bool IsValidDocument(document::IDocument* doc) override;
    bool IsValidField(const document::IIndexFields* fields) override;
    void FillStatistics(const std::shared_ptr<indexlib::framework::SegmentMetrics>& segmentMetrics) override {}
    void Seal() override {}
    std::string GetIndexName() const override { return _attrConfig->GetIndexName(); }
    autil::StringView GetIndexType() const override { return ATTRIBUTE_INDEX_TYPE_STR; }

public:
    virtual const std::shared_ptr<AttributeMemReader> CreateInMemReader() const = 0;

    virtual void AddField(docid_t docId, const autil::StringView& attributeValue, bool isNull) = 0;
    virtual bool UpdateField(docid_t docId, const autil::StringView& attributeValue, bool isNull,
                             const uint64_t* hashKey) = 0;
    Status AddDocument(document::IDocument* doc);

    std::shared_ptr<AttributeConfig> GetAttrConfig() const { return _attrConfig; }
    virtual bool IsUpdatable() const { return _attrConfig->IsAttributeUpdatable(); }

public:
    bool TEST_UpdateField(docid_t docId, const autil::StringView& value, bool isNull)
    {
        return UpdateField(docId, value, isNull, /*hashKey*/ nullptr);
    }
    bool TEST_UpdateField(docid_t docId, const autil::StringView& value, bool isNull, const uint64_t* hashKey)
    {
        return UpdateField(docId, value, isNull, hashKey);
    }

protected:
    using AllocatorPtr = std::shared_ptr<autil::mem_pool::ChunkAllocatorBase>;

    std::shared_ptr<AttributeConfig> _attrConfig;
    AllocatorPtr _allocator;
    std::shared_ptr<autil::mem_pool::Pool> _pool;
    indexlib::util::SimplePool _simplePool;
    std::shared_ptr<AttributeConvertor> _attrConvertor;
    MemIndexerParameter _indexerParam;
    std::unique_ptr<document::extractor::IDocumentInfoExtractor> _docInfoExtractor;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
