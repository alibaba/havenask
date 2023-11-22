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
#include "indexlib/index/field_meta/SourceFieldReader.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/index/attribute/AttributeIndexFactory.h"
#include "indexlib/index/common/FieldTypeTraits.h"
#include "indexlib/index/field_meta/Common.h"
#include "indexlib/index/field_meta/SourceFieldConfigGenerator.h"

namespace indexlib::index {

template <FieldMetaConfig::MetaSourceType sourceType>
Status SourceFieldReader<sourceType>::Open(const std::shared_ptr<FieldMetaConfig>& indexConfig,
                                           const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory)
{
    _config = indexConfig;
    auto fieldConfig = indexConfig->GetFieldConfig();
    if constexpr (sourceType == FieldMetaConfig::MetaSourceType::MST_NONE) {
        return Status::OK();
    }

    assert(sourceType == indexConfig->GetStoreMetaSourceType());
    auto [status, fieldMetaConfig] = SourceFieldConfigGenerator::CreateSourceFieldConfig(_config);
    RETURN_IF_STATUS_ERROR(status, "create attribute config for index [%s] failed", _config->GetIndexName().c_str());
    auto attributeConfig = std::dynamic_pointer_cast<indexlibv2::index::AttributeConfig>(fieldMetaConfig);
    assert(attributeConfig);
    indexlibv2::index::AttributeIndexFactory factory;
    _metaSourceReader =
        std::dynamic_pointer_cast<MetaSourceReader>(factory.CreateDiskIndexer(attributeConfig, _indexerParam));
    if (!_metaSourceReader) {
        return Status::Corruption("create meta source reader failed for index [%s]",
                                  indexConfig->GetIndexName().c_str());
    }
    RETURN_IF_STATUS_ERROR(_metaSourceReader->Open(attributeConfig, indexDirectory),
                           "meta source reader open failed for index [%s]", indexConfig->GetIndexName().c_str());

    if (attributeConfig->IsMultiValue() || attributeConfig->GetFieldType() == ft_string) {
        _maxDataLen = _metaSourceReader->GetAttributeDataInfo().maxItemLen;
    } else {
        _maxDataLen = SizeOfFieldType(attributeConfig->GetFieldType());
    }
    return Status::OK();
}

template <FieldMetaConfig::MetaSourceType sourceType>
size_t SourceFieldReader<sourceType>::EstimateMemUsed(
    const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
    const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory) const
{
    if constexpr (sourceType == FieldMetaConfig::MetaSourceType::MST_NONE) {
        return 0;
    }
    if (_indexerParam.docCount == 0) {
        return 0;
    }
    auto fieldMetaConfig = std::dynamic_pointer_cast<FieldMetaConfig>(indexConfig);
    auto [status, fieldSourceConfig] = SourceFieldConfigGenerator::CreateSourceFieldConfig(fieldMetaConfig);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "create source field config failed");
        return 0;
    }
    auto indexDir = indexlib::file_system::IDirectory::ToLegacyDirectory(indexDirectory)
                        ->GetDirectory(fieldMetaConfig->GetIndexName(), /*throwExceptionIfNotExist=*/false);
    if (indexDir == nullptr) {
        AUTIL_LOG(ERROR, "get index dir [%s] failed", fieldMetaConfig->GetIndexName().c_str());
        return 0;
    }
    auto sourceFieldDir = indexDir->GetDirectory(fieldSourceConfig->GetIndexName(), /*throwExceptionIfNotExist=*/false);
    if (sourceFieldDir == nullptr) {
        AUTIL_LOG(ERROR, "get index dir [%s] failed", fieldSourceConfig->GetIndexName().c_str());
        return 0;
    }
    return sourceFieldDir->EstimateFileMemoryUse(ATTRIBUTE_DATA_FILE_NAME, indexlib::file_system::FSOT_LOAD_CONFIG) +
           sourceFieldDir->EstimateFileMemoryUse(ATTRIBUTE_OFFSET_FILE_NAME, indexlib::file_system::FSOT_LOAD_CONFIG);
}

template <FieldMetaConfig::MetaSourceType sourceType>
size_t SourceFieldReader<sourceType>::EvaluateCurrentMemUsed() const
{
    if constexpr (sourceType == FieldMetaConfig::MetaSourceType::MST_NONE) {
        return 0;
    }
    return _metaSourceReader->EvaluateCurrentMemUsed();
}

template <FieldMetaConfig::MetaSourceType sourceType>
void SourceFieldReader<sourceType>::PrepareReadContext()
{
    if (_metaSourceReader) {
        _metaSourceReader->EnableGlobalReadContext();
    }
}

template <FieldMetaConfig::MetaSourceType sourceType>
bool SourceFieldReader<sourceType>::GetFieldValue(int64_t key, autil::mem_pool::Pool* pool, std::string& field,
                                                  bool& isNull)
{
    if (sourceType == FieldMetaConfig::MetaSourceType::MST_NONE) {
        AUTIL_LOG(ERROR, "not support read source field for index [%s] failed", _config->GetIndexName().c_str());
        return false;
    }
    const docid_t& docId = key;
    bool ret = _metaSourceReader->Read(docId, &field, pool);
    if (!ret) {
        AUTIL_LOG(ERROR, "read doc [%d] field value for index [%s] failed", docId, _config->GetIndexName().c_str());
        return ret;
    }
    if (field == _config->GetFieldConfig()->GetNullFieldLiteralString()) {
        isNull = true;
    } else {
        isNull = false;
    }
    return true;
}

template <FieldMetaConfig::MetaSourceType sourceType>
bool SourceFieldReader<sourceType>::GetFieldTokenCount(int64_t key, autil::mem_pool::Pool* pool,
                                                       uint64_t& fieldTokenCount) const

{
    if constexpr (sourceType != FieldMetaConfig::MetaSourceType::MST_TOKEN_COUNT) {
        AUTIL_LOG(ERROR, "index [%s] config not support get field token count", _config->GetIndexName().c_str());
        return false;
    }
    const auto& docId = key;
    auto ctx = _metaSourceReader->CreateReadContextPtr(pool);
    uint32_t dataLen = 0;
    bool isNull = false;
    bool ret = _metaSourceReader->Read(docId, ctx, (uint8_t*)(&fieldTokenCount), _maxDataLen, dataLen, isNull);
    if (!ret) {
        AUTIL_LOG(ERROR, "read doc [%ld] field token count failed for index [%s]", docId,
                  _config->GetIndexName().c_str());
        return ret;
    }
    return ret;
}

template class SourceFieldReader<FieldMetaConfig::MetaSourceType::MST_NONE>;
template class SourceFieldReader<FieldMetaConfig::MetaSourceType::MST_FIELD>;
template class SourceFieldReader<FieldMetaConfig::MetaSourceType::MST_TOKEN_COUNT>;

} // namespace indexlib::index
