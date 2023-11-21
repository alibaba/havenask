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
#include "indexlib/index/field_meta/SourceFieldIndexFactory.h"

#include "indexlib/base/FieldTypeUtil.h"
#include "indexlib/index/field_meta/Common.h"
#include "indexlib/index/field_meta/MultiValueSourceFieldMerger.h"
#include "indexlib/index/field_meta/SingleValueSourceFieldMerger.h"
#include "indexlib/index/field_meta/SourceFieldConfigGenerator.h"
#include "indexlib/index/field_meta/SourceFieldReader.h"
#include "indexlib/index/field_meta/config/FieldMetaConfig.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, SourceFieldIndexFactory);

SourceFieldIndexFactory::SourceFieldIndexFactory() {}

SourceFieldIndexFactory::~SourceFieldIndexFactory() {}

std::shared_ptr<indexlibv2::index::IIndexMerger>
SourceFieldIndexFactory::CreateSourceFieldMerger(const std::shared_ptr<FieldMetaConfig>& config)
{
#define CREATE_SINGLE_VALUE_SOURCE_MERGER(fieldType_)                                                                  \
    do {                                                                                                               \
        if (fieldType == fieldType_) {                                                                                 \
            return std::make_shared<                                                                                   \
                SingleValueSourceFieldMerger<IndexlibFieldType2CppType<fieldType_, false>::CppType>>(config);          \
        }                                                                                                              \
    } while (0)

    auto [status, sourceFieldConfig] = SourceFieldConfigGenerator::CreateSourceFieldConfig(config);
    if (!status.IsOK()) {
        AUTIL_LOG(ERROR, "create attribute config for index[%s] failed", config->GetIndexName().c_str());
        return nullptr;
    }

    assert(sourceFieldConfig->GetFieldConfigs().size() == 1);
    auto fieldConfig = sourceFieldConfig->GetFieldConfigs()[0];
    auto fieldType = fieldConfig->GetFieldType();
    if (fieldConfig->IsMultiValue()) {
        assert(false);
        AUTIL_LOG(ERROR, "create source merger for index[%s] failed, not support multi value field",
                  config->GetIndexName().c_str());
        return nullptr;
    } else if (fieldType == ft_string) {
        return std::make_shared<MultiValueSourceFieldMerger<char>>(config);
    } else {
        CREATE_SINGLE_VALUE_SOURCE_MERGER(ft_int8);
        CREATE_SINGLE_VALUE_SOURCE_MERGER(ft_int16);
        CREATE_SINGLE_VALUE_SOURCE_MERGER(ft_int32);
        CREATE_SINGLE_VALUE_SOURCE_MERGER(ft_int64);
        CREATE_SINGLE_VALUE_SOURCE_MERGER(ft_uint8);
        CREATE_SINGLE_VALUE_SOURCE_MERGER(ft_uint16);
        CREATE_SINGLE_VALUE_SOURCE_MERGER(ft_uint32);
        CREATE_SINGLE_VALUE_SOURCE_MERGER(ft_uint64);
        CREATE_SINGLE_VALUE_SOURCE_MERGER(ft_float);
        CREATE_SINGLE_VALUE_SOURCE_MERGER(ft_double);
    }

#undef CREATE_SINGLE_VALUE_SOURCE_MERGER

    return nullptr;
}
std::shared_ptr<ISourceFieldReader>
SourceFieldIndexFactory::CreateSourceFieldReader(FieldMetaConfig::MetaSourceType sourceType,
                                                 const indexlibv2::index::DiskIndexerParameter& indexerParam)
{
    if (sourceType == FieldMetaConfig::MetaSourceType::MST_FIELD) {
        return std::make_shared<SourceFieldReader<FieldMetaConfig::MetaSourceType::MST_FIELD>>(indexerParam);
    }
    if (sourceType == FieldMetaConfig::MetaSourceType::MST_TOKEN_COUNT) {
        return std::make_shared<SourceFieldReader<FieldMetaConfig::MetaSourceType::MST_TOKEN_COUNT>>(indexerParam);
    }
    if (sourceType == FieldMetaConfig::MetaSourceType::MST_NONE) {
        return std::make_shared<SourceFieldReader<FieldMetaConfig::MetaSourceType::MST_NONE>>(indexerParam);
    }
    assert(false);
    return nullptr;
}

} // namespace indexlib::index
