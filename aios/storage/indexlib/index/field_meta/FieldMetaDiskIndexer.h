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
#include "indexlib/index/DiskIndexerParameter.h"
#include "indexlib/index/IDiskIndexer.h"
#include "indexlib/index/field_meta/ISourceFieldReader.h"
#include "indexlib/index/field_meta/meta/IFieldMeta.h"

namespace indexlib::index {

class FieldMetaDiskIndexer : public indexlibv2::index::IDiskIndexer
{
public:
    FieldMetaDiskIndexer() {}
    FieldMetaDiskIndexer(const indexlibv2::index::DiskIndexerParameter& indexerParam) : _indexerParam(indexerParam) {}
    ~FieldMetaDiskIndexer() = default;

public:
    Status Open(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory) override;
    size_t EstimateMemUsed(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                           const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory) override;
    size_t EvaluateCurrentMemUsed() override;
    bool GetFieldTokenCount(int64_t key, autil::mem_pool::Pool* pool, uint64_t& fieldTokenCount);
    std::shared_ptr<IFieldMeta> GetSegmentFieldMeta(const std::string& fieldMetaType) const;

public:
    // for merge
    std::pair<Status, std::shared_ptr<ISourceFieldReader>> GetSourceFieldReader();

private:
    FieldMetaConfig::MetaSourceType GetSourceFieldReaderOpenType(const std::shared_ptr<FieldMetaConfig>& config) const;

private:
    std::unordered_map<std::string, std::shared_ptr<IFieldMeta>> _fieldMetas;
    std::shared_ptr<indexlib::file_system::IDirectory> _indexDirectory;
    indexlibv2::index::DiskIndexerParameter _indexerParam;
    std::shared_ptr<ISourceFieldReader> _sourceFieldTokenCountReader;
    std::shared_ptr<FieldMetaConfig> _config;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
