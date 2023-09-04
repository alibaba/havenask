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
#include "indexlib/config/IndexConfigDeserializeResource.h"

#include "indexlib/config/FieldConfig.h"
#include "indexlib/config/FileCompressConfigV2.h"
#include "indexlib/config/SingleFileCompressConfig.h"

namespace indexlibv2::config {

AUTIL_LOG_SETUP(indexlib.config, IndexConfigDeserializeResource);

IndexConfigDeserializeResource::IndexConfigDeserializeResource(
    const std::vector<std::shared_ptr<FieldConfig>>& fieldConfigs, MutableJson& runtimeSettings)
    : _runtimeSettings(runtimeSettings)
{
    for (const auto& fieldConfig : fieldConfigs) {
        _fieldConfigs[fieldConfig->GetFieldName()] = fieldConfig;
    }
}

const std::shared_ptr<FieldConfig>& IndexConfigDeserializeResource::GetFieldConfig(const std::string& fieldName) const
{
    auto iter = _fieldConfigs.find(fieldName);
    if (iter == _fieldConfigs.end()) {
        static const std::shared_ptr<FieldConfig> NULLPTR;
        return NULLPTR;
    }
    return iter->second;
}

std::shared_ptr<config::FileCompressConfigV2>
IndexConfigDeserializeResource::GetFileCompressConfig(const std::string& compressName) const
{
    if (!_fileCompressVec) {
        auto fileCompressVec = GetRuntimeSetting<std::shared_ptr<std::vector<config::SingleFileCompressConfig>>>(
            config::SingleFileCompressConfig::FILE_COMPRESS_CONFIG_KEY);
        if (!fileCompressVec || !*fileCompressVec) {
            AUTIL_LOG(ERROR, "get [%s] failed from settings",
                      config::SingleFileCompressConfig::FILE_COMPRESS_CONFIG_KEY.c_str());
            return nullptr;
        }
        auto status = config::SingleFileCompressConfig::ValidateConfigs(**fileCompressVec);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "validate file compress config failed [%s]", status.ToString().c_str());
            return nullptr;
        }
        _fileCompressVec = *fileCompressVec;
    }
    auto iter =
        std::find_if(_fileCompressVec->begin(), _fileCompressVec->end(), [&compressName](const auto& compressConfig) {
            return compressConfig.GetCompressName() == compressName;
        });
    if (iter == _fileCompressVec->end()) {
        AUTIL_LOG(ERROR, "no [%s] in [%s]", compressName.c_str(),
                  config::SingleFileCompressConfig::FILE_COMPRESS_CONFIG_KEY.c_str());
        return nullptr;
    }
    return std::make_shared<config::FileCompressConfigV2>(_fileCompressVec, compressName);
}

} // namespace indexlibv2::config
