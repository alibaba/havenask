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

#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Status.h"
#include "indexlib/util/KeyValueMap.h"

namespace indexlibv2::config {
class SingleFileCompressConfig;
}

namespace indexlib::config {

class FileCompressSchema;

class FileCompressConfig : public autil::legacy::Jsonizable
{
public:
    FileCompressConfig() = default;
    ~FileCompressConfig();
    FileCompressConfig(const std::string& compressName);

    FileCompressConfig(const FileCompressConfig&) = delete;
    FileCompressConfig& operator=(const FileCompressConfig&) = delete;
    FileCompressConfig(FileCompressConfig&&) = delete;
    FileCompressConfig& operator=(FileCompressConfig&&) = delete;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

public:
    const std::string& GetCompressType(const std::string& temperatureLayer = "") const;
    uint64_t GetCompressBufferSize(const std::string& temperatureLayer = "") const;
    const std::string& GetExcludePattern(const std::string& temperatureLayer = "") const;
    const indexlib::util::KeyValueMap& GetParameters(const std::string& temperatureLayer = "") const;

    Status CheckEqual(const FileCompressConfig& other) const;

    const std::string& GetCompressName() const;
    std::vector<std::string> GetTemperatureFileCompressorNames() const;
    void SetSchema(FileCompressSchema* schema) { _schema = schema; }

    uint64_t GetGlobalFingerPrint() const;

    std::unique_ptr<indexlibv2::config::SingleFileCompressConfig> CreateSingleFileCompressConfig() const;

public:
    void TEST_SetExcludePattern(const std::string& pattern);
    void TEST_SetParameter(const std::string& key, const std::string& value);

private:
    std::shared_ptr<FileCompressConfig> GetConfigForTemperatureLayer(const std::string& layer) const;

private:
    static const size_t DEFAULT_COMPRESS_BUFFER_SIZE;
    std::string _compressName;
    std::string _compressType;
    std::string _excludePattern;
    uint64_t _compressBufferSize;
    indexlib::util::KeyValueMap _compressParameters;
    indexlib::util::KeyValueMap _temperatureCompressorName;
    FileCompressSchema* _schema = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::config
