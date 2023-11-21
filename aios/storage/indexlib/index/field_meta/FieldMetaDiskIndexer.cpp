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
#include "indexlib/index/field_meta/FieldMetaDiskIndexer.h"

#include "indexlib/index/field_meta/SourceFieldIndexFactory.h"
#include "indexlib/index/field_meta/SourceFieldReader.h"
#include "indexlib/index/field_meta/meta/MetaFactory.h"
#include "indexlib/index/field_meta/meta/MinMaxFieldMeta.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, FieldMetaDiskIndexer);

Status FieldMetaDiskIndexer::Open(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                                  const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory)
{
    auto fieldMetaConfig = std::dynamic_pointer_cast<FieldMetaConfig>(indexConfig);
    if (!fieldMetaConfig) {
        return Status::Corruption("cast index config [%s] to field meta config failed",
                                  indexConfig->GetIndexName().c_str());
    }
    _config = fieldMetaConfig;
    if (_indexerParam.docCount == 0) {
        auto [status, fieldMetas] = MetaFactory::CreateFieldMetas(fieldMetaConfig);
        RETURN_IF_STATUS_ERROR(status, "create field metas failed for index [%s]",
                               fieldMetaConfig->GetIndexName().c_str());
        for (const auto& fieldMeta : fieldMetas) {
            auto metaName = fieldMeta->GetFieldMetaName();
            _fieldMetas[metaName] = fieldMeta;
        }
        return Status::OK();
    }

    if (!indexDirectory) {
        RETURN_STATUS_ERROR(Corruption, "open without directory information [%s]",
                            indexDirectory->DebugString().c_str());
    }
    auto indexName = fieldMetaConfig->GetIndexName();
    auto [status, subIDir] = indexDirectory->GetDirectory(indexConfig->GetIndexName()).StatusWith();
    if (!status.IsOK() || !subIDir) {
        AUTIL_LOG(ERROR, "fail to get subDir, indexName[%s] indexType[%s]", indexConfig->GetIndexName().c_str(),
                  indexConfig->GetIndexType().c_str());
        return Status::Corruption("directory for field meta[%s] does not exist", indexConfig->GetIndexName().c_str());
    }
    _indexDirectory = subIDir;
    auto [status2, fieldMetas] = MetaFactory::CreateFieldMetas(fieldMetaConfig);
    RETURN_IF_STATUS_ERROR(status2, "create field metas for [%s] failed", indexName.c_str());
    for (const auto& fieldMeta : fieldMetas) {
        const std::string& metaName = fieldMeta->GetFieldMetaName();
        status2 = fieldMeta->Load(subIDir);
        RETURN_IF_STATUS_ERROR(status2, "field meta [%s] load failed", metaName.c_str());
        _fieldMetas[metaName] = fieldMeta;
    }
    auto openType = GetSourceFieldReaderOpenType(fieldMetaConfig);
    _sourceFieldTokenCountReader = SourceFieldIndexFactory::CreateSourceFieldReader(openType, _indexerParam);
    assert(_sourceFieldTokenCountReader);
    RETURN_IF_STATUS_ERROR(_sourceFieldTokenCountReader->Open(fieldMetaConfig, subIDir), "source reader open failed");
    return Status::OK();
}

FieldMetaConfig::MetaSourceType
FieldMetaDiskIndexer::GetSourceFieldReaderOpenType(const std::shared_ptr<FieldMetaConfig>& config) const
{
    return config->SupportTokenCount() ? config->GetStoreMetaSourceType() : FieldMetaConfig::MetaSourceType::MST_NONE;
}

size_t FieldMetaDiskIndexer::EstimateMemUsed(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                                             const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory)
{
    auto fieldMetaConfig = std::dynamic_pointer_cast<FieldMetaConfig>(indexConfig);
    assert(fieldMetaConfig);
    auto openType = GetSourceFieldReaderOpenType(fieldMetaConfig);
    auto reader = SourceFieldIndexFactory::CreateSourceFieldReader(openType, _indexerParam);
    return reader->EstimateMemUsed(indexConfig, indexDirectory);
}

bool FieldMetaDiskIndexer::GetFieldTokenCount(int64_t key, autil::mem_pool::Pool* pool, uint64_t& fieldTokenCount)
{
    return _sourceFieldTokenCountReader->GetFieldTokenCount(key, pool, fieldTokenCount);
}
size_t FieldMetaDiskIndexer::EvaluateCurrentMemUsed()
{
    return _indexerParam.docCount == 0 ? 0 : _sourceFieldTokenCountReader->EvaluateCurrentMemUsed();
}

std::pair<Status, std::shared_ptr<ISourceFieldReader>> FieldMetaDiskIndexer::GetSourceFieldReader()
{
    auto sourceType = _config->GetStoreMetaSourceType();
    if (sourceType == FieldMetaConfig::MetaSourceType::MST_NONE) {
        assert(false);
        return {Status::Corruption("get source field reader for index [%s]", _config->GetIndexName().c_str()), nullptr};
    }
    std::shared_ptr<ISourceFieldReader> reader;
    switch (sourceType) {
    case FieldMetaConfig::MetaSourceType::MST_FIELD:
        reader = std::make_shared<SourceFieldReader<FieldMetaConfig::MetaSourceType::MST_FIELD>>(_indexerParam);
        break;
    case FieldMetaConfig::MetaSourceType::MST_TOKEN_COUNT:
        reader = std::make_shared<SourceFieldReader<FieldMetaConfig::MetaSourceType::MST_TOKEN_COUNT>>(_indexerParam);
        break;
    default:
        assert(false);
        return {Status::Corruption("get source field reader for index [%s]", _config->GetIndexName().c_str()), nullptr};
    }
    RETURN2_IF_STATUS_ERROR(reader->Open(_config, _indexDirectory), nullptr, "source reader open failed");
    return {Status::OK(), reader};
}

std::shared_ptr<IFieldMeta> FieldMetaDiskIndexer::GetSegmentFieldMeta(const std::string& fieldMetaType) const
{
    auto iter = _fieldMetas.find(fieldMetaType);
    if (iter != _fieldMetas.end()) {
        return iter->second;
    }
    AUTIL_LOG(ERROR, "fieldMetaType [%s] not found", fieldMetaType.c_str());
    return nullptr;
}

} // namespace indexlib::index
