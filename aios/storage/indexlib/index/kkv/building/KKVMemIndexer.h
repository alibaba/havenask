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
#include "autil/MultiValueFormatter.h"
#include "indexlib/document/kkv/KKVDocumentBatch.h"
#include "indexlib/index/IMemIndexer.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertorFactory.h"
#include "indexlib/index/common/field_format/attribute/CompactPackAttributeDecoder.h"
#include "indexlib/index/common/field_format/pack_attribute/PackAttributeFormatter.h"
#include "indexlib/index/kkv/Constant.h"
#include "indexlib/index/kkv/building/KKVBuildingSegmentReader.h"
#include "indexlib/index/kkv/building/KKVValueWriter.h"
#include "indexlib/index/kkv/building/SKeyWriter.h"
#include "indexlib/index/kkv/common/Trait.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "indexlib/index/kkv/pkey_table/PrefixKeyTableBase.h"
#include "indexlib/index/kkv/pkey_table/PrefixKeyTableCreator.h"
#include "indexlib/index/kkv/pkey_table/PrefixKeyTableSeeker.h"
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

template <typename SKeyType>
class KKVMemIndexer : public IMemIndexer
{
private:
    using PKeyTable = PrefixKeyTableBase<SKeyListInfo>;

public:
    KKVMemIndexer(const std::string& tabletName, size_t maxPKeyMemUse, size_t maxSKeyMemUse, size_t maxValueMemUse,
                  double skeyCompressRatio, double valueCompressRatio, bool isOnline, bool tolerateDocError);
    ~KKVMemIndexer() = default;

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

    std::string GetIndexName() const override { return _indexConfig->GetIndexName(); }
    autil::StringView GetIndexType() const override { return KKV_INDEX_TYPE_STR; }
    uint64_t GetIndexNameHash() const { return _indexNameHash; }

    void Seal() override;
    bool IsDirty() const override;

public:
    std::shared_ptr<KKVBuildingSegmentReader<SKeyType>> CreateInMemoryReader() const;

public:
    const std::shared_ptr<PKeyTable> TEST_GetPKeyTable() const { return _pkeyTable; }
    const std::shared_ptr<SKeyWriter<SKeyType>> TEST_GetSkeyWriter() const { return _skeyWriter; }
    const std::shared_ptr<KKVValueWriter> TEST_GetValueWriter() const { return _valueWriter; }

private:
    bool IsFull();
    Status BuildSingleDocument(const document::KKVDocument* doc, size_t& valueSize);
    Status AddDocument(const document::KKVDocument* doc);
    Status DeleteDocument(const document::KKVDocument* doc);
    Status DeleteSKey(const document::KKVDocument* doc);
    Status DeletePKey(const document::KKVDocument* doc);

private:
    Status DecodeDocValue(autil::StringView& value);
    Status InitValueWriter(size_t maxValueMemUse);
    uint32_t GetExpireTime(const document::KKVDocument* doc) const;
    size_t EstimatePkeyCount(size_t maxPKeyMemUse, config::KKVIndexConfig* indexConfig);
    void UpdateBuildResourceMetrics(size_t& valueLen);
    size_t EstimateDumpFileSize() const;
    size_t EstimateDumpTmpMemUse() const;
    Status ConvertBuildDocStatus(const Status& status) const;

private:
    std::shared_ptr<PKeyTable> _pkeyTable;
    std::shared_ptr<SKeyWriter<SKeyType>> _skeyWriter;
    std::shared_ptr<KKVValueWriter> _valueWriter;
    std::shared_ptr<AttributeConvertor> _attrConvertor;
    std::shared_ptr<PlainFormatEncoder> _plainFormatEncoder;

    std::string _tabletName;
    std::string _indexName;
    uint64_t _indexNameHash = 0;
    std::shared_ptr<indexlibv2::config::KKVIndexConfig> _indexConfig;
    int32_t _fixValueLen = -1;
    indexlib::util::MemBuffer _memBuffer;

    size_t _maxValueLen = 0;
    size_t _maxPKeyMemUse = 0;
    size_t _maxSKeyMemUse = 0;
    size_t _maxValueMemUse = 0;
    double _skeyCompressRatio = 1.0;
    double _valueCompressRatio = 1.0;
    bool _isOnline = false;
    bool _tolerateDocError = true;

public:
    // pkeyMem / totalMem
    static constexpr double DEFAULT_PKEY_MEMORY_USE_RATIO = 0.1;
    // skeyMem / (skeyMem + valueMem)
    static constexpr double DEFAULT_SKEY_VALUE_MEM_RATIO = 0.01;
    static constexpr double MIN_SKEY_VALUE_MEM_RATIO = 0.0001;
    static constexpr double MAX_SKEY_VALUE_MEM_RATIO = 0.90;
    static constexpr double SKEY_NEW_MEMORY_RATIO_WEIGHT = 0.7;
    static constexpr double DEFAULT_SKEY_COMPRESS_RATIO = 1.0;
    static constexpr double DEFAULT_VALUE_COMPRESS_RATIO = 1.0;

private:
    AUTIL_LOG_DECLARE();
};

MARK_EXPLICIT_DECLARE_ALL_SKEY_TYPE_TEMPLATE_CALSS(KKVMemIndexer);

} // namespace indexlibv2::index
