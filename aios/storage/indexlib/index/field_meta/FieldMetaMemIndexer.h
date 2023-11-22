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
#include "indexlib/index/common/field_format/field_meta/FieldMetaConvertor.h"
#include "indexlib/index/field_meta/Common.h"
#include "indexlib/index/field_meta/SourceFieldWriter.h"
#include "indexlib/index/field_meta/config/FieldMetaConfig.h"
#include "indexlib/index/field_meta/meta/IFieldMeta.h"

namespace indexlib::index {

class FieldMetaMemIndexer : public indexlibv2::index::IMemIndexer
{
public:
    FieldMetaMemIndexer() {}
    ~FieldMetaMemIndexer() = default;

public:
    Status Init(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                indexlibv2::document::extractor::IDocumentInfoExtractorFactory* docInfoExtractorFactory) override;
    Status Build(indexlibv2::document::IDocumentBatch* docBatch) override;
    Status Build(const indexlibv2::document::IIndexFields* indexFields, size_t n) override;
    void ValidateDocumentBatch(indexlibv2::document::IDocumentBatch* docBatch) override;
    bool IsValidDocument(indexlibv2::document::IDocument* doc) override;
    bool IsValidField(const indexlibv2::document::IIndexFields* fields) override;
    void FillStatistics(const std::shared_ptr<indexlib::framework::SegmentMetrics>& segmentMetrics) override {}
    void Seal() override {}
    std::string GetIndexName() const override;
    autil::StringView GetIndexType() const override { return FIELD_META_INDEX_TYPE_STR; }

    Status Dump(autil::mem_pool::PoolBase* dumpPool,
                const std::shared_ptr<indexlib::file_system::Directory>& indexDirectory,
                const std::shared_ptr<indexlibv2::framework::DumpParams>& params) override;
    void UpdateMemUse(indexlibv2::index::BuildingIndexMemoryUseUpdater* memUpdater) override;
    bool IsDirty() const override;

public:
    std::shared_ptr<IFieldMeta> GetSegmentFieldMeta(const std::string& fieldMetaType) const;
    bool GetFieldTokenCount(int64_t key, autil::mem_pool::Pool* pool, uint64_t& fieldTokenCount);

private:
    std::shared_ptr<FieldMetaConfig> _fieldMetaConfig;
    std::shared_ptr<FieldMetaConvertor> _convertor;
    std::vector<std::shared_ptr<IFieldMeta>> _fieldMetas;
    std::unique_ptr<indexlibv2::document::extractor::IDocumentInfoExtractor> _docInfoExtractor;
    std::shared_ptr<ISourceFieldWriter> _sourceWriter;
    bool _isDirty = false;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
