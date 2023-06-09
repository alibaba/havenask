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
#include "indexlib/config/FileCompressConfig.h"

#include "autil/StringUtil.h"
#include "indexlib/config/ConfigDefine.h"
#include "indexlib/config/FileCompressSchema.h"
#include "indexlib/config/SingleFileCompressConfig.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/RegularExpression.h"
#include "indexlib/util/buffer_compressor/BufferCompressorCreator.h"

using namespace std;

namespace indexlib::config {
AUTIL_LOG_SETUP(indexlib.config, FileCompressConfig);

FileCompressConfig::~FileCompressConfig() {}

const size_t FileCompressConfig::DEFAULT_COMPRESS_BUFFER_SIZE = 4 * 1024; // 4KB

FileCompressConfig::FileCompressConfig(const std::string& compressName) : _compressName(compressName) {}

void FileCompressConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("name", _compressName);
    json.Jsonize("type", _compressType);
    json.Jsonize("compress_buffer_size", _compressBufferSize, DEFAULT_COMPRESS_BUFFER_SIZE);
    if (_compressName.empty()) {
        INDEXLIB_FATAL_ERROR(Schema, "file compress config has empty name.");
    }
    if (!_compressType.empty() && !indexlib::util::BufferCompressorCreator::IsValidCompressorName(_compressType)) {
        INDEXLIB_FATAL_ERROR(Schema, "unknow compress type: [%s]", _compressType.c_str());
    }

    if (json.GetMode() == FROM_JSON) {
        json.Jsonize("exclude_file_pattern", _excludePattern, _excludePattern);
        json.Jsonize("parameters", _compressParameters, _compressParameters);
        map<string, string> tmpMap;
        json.Jsonize("temperature_compressor", tmpMap, tmpMap);
        for (auto it : tmpMap) {
            string layerStr = it.first;
            string compressorName = it.second;
            autil::StringUtil::toUpperCase(layerStr);
            if (layerStr != "HOT" && layerStr != "WARM" && layerStr != "COLD") {
                INDEXLIB_FATAL_ERROR(Schema, "unknow temperature type: [%s]", layerStr.c_str());
            }
            _temperatureCompressorName[layerStr] = compressorName;
        }
        if (!_excludePattern.empty() && !indexlib::util::RegularExpression::CheckPattern(_excludePattern)) {
            INDEXLIB_FATAL_ERROR(Schema, "invalid exclude_file_pattern [%s]", _excludePattern.c_str());
        }
    } else {
        if (!_excludePattern.empty()) {
            json.Jsonize("exclude_file_pattern", _excludePattern);
        }
        if (!_temperatureCompressorName.empty()) {
            json.Jsonize("temperature_compressor", _temperatureCompressorName);
        }
        if (!_compressParameters.empty()) {
            json.Jsonize("parameters", _compressParameters);
        }
    }
}

const string& FileCompressConfig::GetCompressName() const { return _compressName; }

std::shared_ptr<FileCompressConfig> FileCompressConfig::GetConfigForTemperatureLayer(const string& layer) const
{
    string unifyLayerStr = layer;
    autil::StringUtil::toUpperCase(unifyLayerStr);

    auto iter = _temperatureCompressorName.find(unifyLayerStr);
    if (iter == _temperatureCompressorName.end()) {
        return std::shared_ptr<FileCompressConfig>();
    }

    std::shared_ptr<FileCompressConfig> temperatureCompressConfig;
    if (_schema) {
        temperatureCompressConfig = _schema->GetFileCompressConfig(iter->second);
    }
    if (!temperatureCompressConfig) {
        INDEXLIB_FATAL_ERROR(Schema, "GetFileCompressConfig for [%s] fail", layer.c_str());
    }
    return temperatureCompressConfig;
}

const string& FileCompressConfig::GetCompressType(const string& temperatureLayer) const
{
    if (temperatureLayer.empty()) {
        return _compressType;
    }
    std::shared_ptr<FileCompressConfig> config = GetConfigForTemperatureLayer(temperatureLayer);
    return config != nullptr ? config->GetCompressType() : _compressType;
}

const string& FileCompressConfig::GetExcludePattern(const string& temperatureLayer) const
{
    if (temperatureLayer.empty()) {
        return _excludePattern;
    }
    std::shared_ptr<FileCompressConfig> config = GetConfigForTemperatureLayer(temperatureLayer);
    return config != nullptr ? config->GetExcludePattern() : _excludePattern;
}

uint64_t FileCompressConfig::GetCompressBufferSize(const string& temperatureLayer) const
{
    if (temperatureLayer.empty()) {
        return _compressBufferSize;
    }
    std::shared_ptr<FileCompressConfig> config = GetConfigForTemperatureLayer(temperatureLayer);
    return config != nullptr ? config->GetCompressBufferSize() : _compressBufferSize;
}

const indexlib::util::KeyValueMap& FileCompressConfig::GetParameters(const string& temperatureLayer) const
{
    if (temperatureLayer.empty()) {
        return _compressParameters;
    }
    std::shared_ptr<FileCompressConfig> config = GetConfigForTemperatureLayer(temperatureLayer);
    return config != nullptr ? config->GetParameters() : _compressParameters;
}

vector<string> FileCompressConfig::GetTemperatureFileCompressorNames() const
{
    vector<string> ret;
    ret.reserve(_temperatureCompressorName.size());
    for (auto it : _temperatureCompressorName) {
        ret.push_back(it.second);
    }
    return ret;
}

uint64_t FileCompressConfig::GetGlobalFingerPrint() const
{
    return _schema != nullptr ? _schema->GetFingerPrint() : FileCompressSchema::INVALID_FINGER_PRINT;
}

Status FileCompressConfig::CheckEqual(const FileCompressConfig& other) const
{
    CHECK_CONFIG_EQUAL(_compressName, other._compressName, "_compressName not equal");
    CHECK_CONFIG_EQUAL(_compressType, other._compressType, "_compressType not equal");
    CHECK_CONFIG_EQUAL(_excludePattern, other._excludePattern, "_excludePattern not equal");
    CHECK_CONFIG_EQUAL(_compressBufferSize, other._compressBufferSize, "_compressBufferSize not equal");
    CHECK_CONFIG_EQUAL(_compressParameters, other._compressParameters, "_compressParameters not equal");
    CHECK_CONFIG_EQUAL(_temperatureCompressorName, other._temperatureCompressorName,
                       "_temperatureCompressorName not equal");
    return Status::OK();
}

void FileCompressConfig::TEST_SetExcludePattern(const std::string& pattern)
{
    _excludePattern = pattern;
    if (_schema) {
        _schema->ResetFingerPrint();
    }
}

void FileCompressConfig::TEST_SetParameter(const std::string& key, const std::string& value)
{
    _compressParameters[key] = value;
    if (_schema) {
        _schema->ResetFingerPrint();
    }
}

std::unique_ptr<indexlibv2::config::SingleFileCompressConfig> FileCompressConfig::CreateSingleFileCompressConfig() const
{
    if (!_temperatureCompressorName.empty()) {
        AUTIL_LOG(ERROR, "can not convert temperature config to v2 file compress config");
        return nullptr;
    }
    auto singleFileCompressConfig =
        std::make_unique<indexlibv2::config::SingleFileCompressConfig>(_compressName, _compressType);
    singleFileCompressConfig->_excludePattern = _excludePattern;
    singleFileCompressConfig->_compressBufferSize = _compressBufferSize;
    singleFileCompressConfig->_compressParameters = _compressParameters;
    return singleFileCompressConfig;
}

} // namespace indexlib::config
