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
#include "autil/NoCopyable.h"
#include "indexlib/document/extractor/plain/DocumentInfoExtractorFactory.h"
#include "indexlib/index/attribute/AttributeMemIndexer.h"
#include "indexlib/index/common/field_format/attribute/SingleValueAttributeConvertor.h"
#include "indexlib/index/field_meta/ISourceFieldWriter.h"
#include "indexlib/index/field_meta/config/FieldMetaConfig.h"

namespace indexlib::index {

template <FieldMetaConfig::MetaSourceType sourceType>
class SourceFieldWriter : public ISourceFieldWriter
{
public:
    SourceFieldWriter(indexlibv2::document::extractor::IDocumentInfoExtractorFactory* docInfoExtractorFactor)
        : _factory(docInfoExtractorFactor)
    {
    }
    ~SourceFieldWriter() = default;

    using MetaSourceWriter = indexlibv2::index::AttributeMemIndexer;
    using SourceFieldConvertor = indexlibv2::index::AttributeConvertor;

public:
    Status Init(const std::shared_ptr<FieldMetaConfig>& config) override;
    Status Build(const FieldValueBatch& docBatch) override;
    Status Dump(autil::mem_pool::PoolBase* dumpPool,
                const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory,
                const std::shared_ptr<indexlibv2::framework::DumpParams>& params) override;
    bool GetFieldTokenCount(int64_t key, autil::mem_pool::Pool* pool, uint64_t& fieldTokenCount) override;
    void UpdateMemUse(indexlibv2::index::BuildingIndexMemoryUseUpdater* memUpdater) override;

private:
    autil::mem_pool::Pool _pool;
    std::shared_ptr<MetaSourceWriter> _metaSourceWriter;
    std::shared_ptr<FieldMetaConfig> _fieldMetaConfig;
    std::shared_ptr<SourceFieldConvertor> _sourceFieldConvertor;
    indexlibv2::document::extractor::IDocumentInfoExtractorFactory* _factory;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE_WITH_TYPENAME(indexlib.index, SourceFieldWriter, FieldMetaConfig::MetaSourceType, sourceType);
} // namespace indexlib::index
