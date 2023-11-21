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
#include "indexlib/index/field_meta/SourceFieldWriter.h"

#include "indexlib/file_system/IDirectory.h"
#include "indexlib/index/attribute/AttributeIndexFactory.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertorFactory.h"
#include "indexlib/index/common/field_format/field_meta/FieldMetaConvertor.h"
#include "indexlib/index/field_meta/Common.h"
#include "indexlib/index/field_meta/SourceFieldConfigGenerator.h"

namespace indexlib::index {

template <FieldMetaConfig::MetaSourceType sourceType>
Status SourceFieldWriter<sourceType>::Init(const std::shared_ptr<FieldMetaConfig>& config)
{
    _fieldMetaConfig = config;
    assert(sourceType == config->GetStoreMetaSourceType());
    if constexpr (sourceType == FieldMetaConfig::MetaSourceType::MST_NONE) {
        return Status::OK();
    }
    auto fieldConfig = config->GetFieldConfig();
    auto fieldName = fieldConfig->GetFieldName();
    auto [status, sourceFieldConfig] = SourceFieldConfigGenerator::CreateSourceFieldConfig(config);
    RETURN_IF_STATUS_ERROR(status, "create attribute config for index [%s] failed", config->GetIndexName().c_str());
    auto attributeConfig = std::dynamic_pointer_cast<indexlibv2::index::AttributeConfig>(sourceFieldConfig);
    assert(attributeConfig);
    _sourceFieldConvertor = std::shared_ptr<indexlibv2::index::AttributeConvertor>(
        indexlibv2::index::AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attributeConfig));
    if (!_sourceFieldConvertor) {
        assert(false);
        return Status::Corruption("create attribute convertor for index [%s] failed", config->GetIndexName().c_str());
    }
    indexlibv2::index::MemIndexerParameter param;
    indexlibv2::index::AttributeIndexFactory factory;
    _metaSourceWriter = std::dynamic_pointer_cast<MetaSourceWriter>(factory.CreateMemIndexer(attributeConfig, param));
    if (!_metaSourceWriter) {
        return Status::Corruption("create meta source failed for index [%s]", config->GetIndexName().c_str());
    }
    return _metaSourceWriter->Init(attributeConfig, _factory);
}

template <FieldMetaConfig::MetaSourceType sourceType>
Status SourceFieldWriter<sourceType>::Build(const FieldValueBatch& fieldValues)
{
    if constexpr (sourceType == FieldMetaConfig::MetaSourceType::MST_NONE) {
        return Status::OK();
    }
    for (const auto& [fieldValueMeta, isNull, docId] : fieldValues) {
        const auto& [fieldValue, tokenCount] = fieldValueMeta;
        bool formatError = false;
        std::string fieldValueEncodeValue;
        if constexpr (sourceType == FieldMetaConfig::MetaSourceType::MST_FIELD) {
            if (!isNull) {
                auto valueStringView = autil::StringView(fieldValue);
                fieldValueEncodeValue = _sourceFieldConvertor->Encode(valueStringView, &_pool, formatError);
                if (formatError) {
                    assert(false);
                    return Status::Corruption("souce field for index [%s] encode value is [%s] error",
                                              _fieldMetaConfig->GetIndexName().c_str(), fieldValue.c_str());
                }
            }
            _metaSourceWriter->AddField(docId, fieldValueEncodeValue, isNull);
        } else {
            std::string tokenCountStr = autil::StringUtil::toString(tokenCount);
            auto tokenCountStringView = autil::StringView(tokenCountStr);
            auto fieldTokenCountStr = _sourceFieldConvertor->Encode(tokenCountStringView, &_pool, formatError);
            if (formatError) {
                assert(false);
                return Status::Corruption("store souce field token count for index [%s] count [%d] encode error",
                                          _fieldMetaConfig->GetIndexName().c_str(), tokenCount);
            }
            _metaSourceWriter->AddField(docId, fieldTokenCountStr, false /*isNull*/);
        }
    }
    _pool.release();
    return Status::OK();
}

template <FieldMetaConfig::MetaSourceType sourceType>
Status SourceFieldWriter<sourceType>::Dump(autil::mem_pool::PoolBase* dumpPool,
                                           const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory,
                                           const std::shared_ptr<indexlibv2::framework::DumpParams>& params)
{
    if constexpr (sourceType == FieldMetaConfig::MetaSourceType::MST_NONE) {
        return Status::OK();
    }
    return _metaSourceWriter->Dump(dumpPool, file_system::IDirectory::ToLegacyDirectory(indexDirectory), params);
}

template <FieldMetaConfig::MetaSourceType sourceType>
bool SourceFieldWriter<sourceType>::GetFieldTokenCount(int64_t key, autil::mem_pool::Pool* pool,
                                                       uint64_t& fieldTokenCount)
{
    if constexpr (sourceType != FieldMetaConfig::MetaSourceType::MST_TOKEN_COUNT) {
        AUTIL_LOG(ERROR, "index [%s] config not support get field token count",
                  _fieldMetaConfig->GetIndexName().c_str());
        return false;
    }
    docid_t docId = key;
    bool isNull = false;
    auto memReader = _metaSourceWriter->CreateInMemReader();
    auto ret = memReader->Read(docId, (uint8_t*)(&fieldTokenCount), sizeof(uint64_t), isNull);
    if (!ret) {
        AUTIL_LOG(ERROR, "read doc [%d] field token count failed for index [%s]", docId,
                  _fieldMetaConfig->GetIndexName().c_str());
        return ret;
    }
    return true;
}

template <FieldMetaConfig::MetaSourceType sourceType>
void SourceFieldWriter<sourceType>::UpdateMemUse(indexlibv2::index::BuildingIndexMemoryUseUpdater* memUpdater)
{
    if constexpr (sourceType == FieldMetaConfig::MetaSourceType::MST_NONE) {
        return;
    }
    return _metaSourceWriter->UpdateMemUse(memUpdater);
}
template class SourceFieldWriter<FieldMetaConfig::MetaSourceType::MST_NONE>;
template class SourceFieldWriter<FieldMetaConfig::MetaSourceType::MST_FIELD>;
template class SourceFieldWriter<FieldMetaConfig::MetaSourceType::MST_TOKEN_COUNT>;

} // namespace indexlib::index
