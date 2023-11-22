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
#include "indexlib/index/attribute/AttributeDiskIndexer.h"
#include "indexlib/index/field_meta/ISourceFieldReader.h"
#include "indexlib/index/field_meta/config/FieldMetaConfig.h"

namespace indexlib::index {

template <FieldMetaConfig::MetaSourceType sourceType>
class SourceFieldReader : public ISourceFieldReader
{
public:
    SourceFieldReader(const indexlibv2::index::DiskIndexerParameter& indexerParam) : _indexerParam(indexerParam) {}
    ~SourceFieldReader() = default;

    using MetaSourceReader = indexlibv2::index::AttributeDiskIndexer;

public:
    Status Open(const std::shared_ptr<FieldMetaConfig>& indexConfig,
                const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory) override;
    bool GetFieldTokenCount(int64_t key, autil::mem_pool::Pool* pool, uint64_t& fieldTokenCount) const override;
    FieldMetaConfig::MetaSourceType GetMetaStoreType() const override { return sourceType; }

public:
    // for merge
    void PrepareReadContext() override;
    bool GetFieldValue(int64_t key, autil::mem_pool::Pool* pool, std::string& field, bool& isNull) override;
    std::shared_ptr<indexlibv2::index::IIndexer> GetSourceFieldDiskIndexer() const override
    {
        return _metaSourceReader;
    }

    size_t EstimateMemUsed(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                           const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory) const override;
    size_t EvaluateCurrentMemUsed() const override;

private:
    indexlibv2::index::DiskIndexerParameter _indexerParam;
    std::shared_ptr<MetaSourceReader> _metaSourceReader;
    std::shared_ptr<FieldMetaConfig> _config;
    uint32_t _maxDataLen = 0;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE_WITH_TYPENAME(indexlib.index, SourceFieldReader, FieldMetaConfig::MetaSourceType, sourceType);
} // namespace indexlib::index
