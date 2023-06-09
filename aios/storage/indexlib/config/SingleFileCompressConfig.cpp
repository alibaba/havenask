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
#include "indexlib/config/SingleFileCompressConfig.h"

#include "indexlib/util/RegularExpression.h"
#include "indexlib/util/buffer_compressor/BufferCompressorCreator.h"

namespace indexlibv2::config {
AUTIL_LOG_SETUP(indexlib.config, SingleFileCompressConfig);

SingleFileCompressConfig::SingleFileCompressConfig() {}
SingleFileCompressConfig::SingleFileCompressConfig(const std::string& compressName, const std::string& compressType)
    : _compressName(compressName)
    , _compressType(compressType)
    , _compressBufferSize(DEFAULT_COMPRESS_BUFFER_SIZE)
{
}

SingleFileCompressConfig::~SingleFileCompressConfig() {}

const size_t SingleFileCompressConfig::DEFAULT_COMPRESS_BUFFER_SIZE = 4 * 1024; // 4KB

void SingleFileCompressConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("name", _compressName);
    json.Jsonize("type", _compressType);
    if (_compressType != COMBINED_COMPRESS_TYPE) {
        json.Jsonize("compress_buffer_size", _compressBufferSize, DEFAULT_COMPRESS_BUFFER_SIZE);
        if (json.GetMode() == FROM_JSON) {
            json.Jsonize("exclude_file_pattern", _excludePattern, _excludePattern);
            json.Jsonize("parameters", _compressParameters, _compressParameters);
        } else {
            if (!_excludePattern.empty()) {
                json.Jsonize("exclude_file_pattern", _excludePattern);
            }
            if (!_compressParameters.empty()) {
                json.Jsonize("parameters", _compressParameters);
            }
            if (!_statisticKey.empty()) {}
        }
    } else {
        json.Jsonize("statistic_key", _statisticKey, _statisticKey);
        json.Jsonize("statistic_values", _statisticValues, _statisticValues);
        json.Jsonize("compressor_names", _compressorNames, _compressorNames);
        json.Jsonize("default_compressor", _defaultCompressor, _defaultCompressor);
    }
}

const std::string& SingleFileCompressConfig::GetCompressName() const { return _compressName; }
const std::string& SingleFileCompressConfig::GetCompressType() const { return _compressType; }
const std::string& SingleFileCompressConfig::GetExcludePattern() const { return _excludePattern; }
uint64_t SingleFileCompressConfig::GetCompressBufferSize() const { return _compressBufferSize; }
const std::map<std::string, std::string>& SingleFileCompressConfig::GetParameters() const
{
    return _compressParameters;
}
const std::string& SingleFileCompressConfig::GetStatisticKey() const { return _statisticKey; }
const std::vector<std::string>& SingleFileCompressConfig::GetStatisticValues() const { return _statisticValues; }
const std::vector<std::string>& SingleFileCompressConfig::GetCompressorNames() const { return _compressorNames; }
const std::string& SingleFileCompressConfig::GetDefaultCompressor() const { return _defaultCompressor; }

Status SingleFileCompressConfig::ValidateConfigs(const std::vector<config::SingleFileCompressConfig>& configs)
{
    for (const auto& config : configs) {
        // check name
        if (config.GetCompressName().empty()) {
            AUTIL_LOG(ERROR, "compress name is empty [%s]", autil::legacy::ToJsonString(configs, true).c_str());
            return Status::InvalidArgs("compress name empty");
        }

        // check type
        const auto& type = config.GetCompressType();
        if (type == COMBINED_COMPRESS_TYPE) {
            auto values = config.GetStatisticValues();
            auto referedCompressors = config.GetCompressorNames();
            if (values.size() != referedCompressors.size()) {
                AUTIL_LOG(ERROR, "values size [%lu] and refered compressor size [%lu] not equal in [%s] compressor",
                          values.size(), referedCompressors.size(), config.GetCompressName().c_str());
                return Status::InvalidArgs("combined compressor invalid");
            }
            for (const auto& compressor : referedCompressors) {
                auto iter = std::find_if(configs.begin(), configs.end(), [compressor](const auto& singleConfig) {
                    return singleConfig.GetCompressName() == compressor;
                });
                if (iter == configs.end() || iter->GetCompressType() == COMBINED_COMPRESS_TYPE) {
                    AUTIL_LOG(ERROR, "compressor [%s] is not normal compressor in config, but refered by [%s]",
                              compressor.c_str(), config.GetCompressName().c_str());
                    return Status::InvalidArgs("combined compressor invalid");
                }
            }
        } else {
            if (!type.empty() && !indexlib::util::BufferCompressorCreator::IsValidCompressorName(type)) {
                AUTIL_LOG(ERROR, "compress type [%s] invalid in  [%s]", type.c_str(),
                          autil::legacy::ToJsonString(configs, true).c_str());
                return Status::InvalidArgs("compress type invalid");
            }
        }

        // check exclude pattern
        const auto& excludePattern = config.GetExcludePattern();
        if (!excludePattern.empty() && !indexlib::util::RegularExpression::CheckPattern(excludePattern)) {
            AUTIL_LOG(ERROR, "exclude pattern [%s] is invalid", excludePattern.c_str());
            return Status::InvalidArgs("exclude pattern invalid");
        }
    }
    return Status::OK();
}
} // namespace indexlibv2::config
