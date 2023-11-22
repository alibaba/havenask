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
#include "indexlib/index/field_meta/SourceFieldConfigGenerator.h"

#include "indexlib/config/FileCompressConfig.h"
#include "indexlib/config/FileCompressConfigV2.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/field_meta/Common.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, SourceFieldConfigGenerator);

std::pair<Status, std::shared_ptr<indexlibv2::config::IIndexConfig>>
SourceFieldConfigGenerator::CreateSourceFieldConfig(const std::shared_ptr<FieldMetaConfig>& config)
{
    auto sourceType = config->GetStoreMetaSourceType();
    if (sourceType == FieldMetaConfig::MetaSourceType::MST_NONE) {
        assert(false);
        return {Status::Corruption("not support"), nullptr};
    }
    auto fieldConfig = config->GetFieldConfig();
    auto attributeConfig = std::make_shared<indexlibv2::index::AttributeConfig>();

    if (sourceType == FieldMetaConfig::MetaSourceType::MST_FIELD) {
        auto status = attributeConfig->Init(fieldConfig);
        RETURN2_IF_STATUS_ERROR(status, nullptr, "attribute init failed");
    } else {
        auto tokenCountConfig = std::make_shared<indexlibv2::config::FieldConfig>(
            fieldConfig->GetFieldName() + SOURCE_TOKEN_COUNT_STORE_SUFFIX, ft_uint64, false);
        tokenCountConfig->SetVirtual(true);
        auto status = attributeConfig->Init(tokenCountConfig);
        RETURN2_IF_STATUS_ERROR(status, nullptr, "field [%s] init attribute config failed",
                                tokenCountConfig->GetFieldName().c_str());
    }
    // make compress
    std::string defaultCompressConfig = R"(
      {
         "name" : "__source_field_compressor__",
         "type" : "zstd",
         "exclude_file_pattern" : "offset"
      }
    )";
    auto fileCompress = std::make_shared<config::FileCompressConfig>();
    autil::legacy::FromJsonString(*fileCompress, defaultCompressConfig);
    attributeConfig->SetFileCompressConfig(fileCompress);

    auto configs = std::make_shared<std::vector<indexlibv2::config::SingleFileCompressConfig>>();
    configs->emplace_back(*fileCompress->CreateSingleFileCompressConfig());

    auto fileCompressV2 =
        std::make_shared<indexlibv2::config::FileCompressConfigV2>(configs, "source_field_compressor");
    attributeConfig->SetFileCompressConfigV2(fileCompressV2);

    return std::make_pair(Status::OK(), attributeConfig);
}

} // namespace indexlib::index
