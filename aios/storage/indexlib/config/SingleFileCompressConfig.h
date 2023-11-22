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

#include <map>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Status.h"

namespace indexlib::config {
class FileCompressConfig;
}

namespace indexlibv2::config {

class SingleFileCompressConfig : public autil::legacy::Jsonizable
{
public:
    SingleFileCompressConfig();
    SingleFileCompressConfig(const std::string& compressName, const std::string& compressType);
    ~SingleFileCompressConfig();

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    static Status ValidateConfigs(const std::vector<config::SingleFileCompressConfig>& configs);

public:
    const std::string& GetCompressName() const;
    const std::string& GetCompressType() const;
    const std::string& GetExcludePattern() const;
    uint64_t GetCompressBufferSize() const;
    const std::map<std::string, std::string>& GetParameters() const;
    const std::string& GetStatisticKey() const;
    const std::vector<std::string>& GetStatisticValues() const;
    const std::vector<std::string>& GetCompressorNames() const;
    const std::string& GetDefaultCompressor() const;

public:
    inline static const std::string FILE_COMPRESS_CONFIG_KEY = "file_compressors";
    inline static const std::string COMBINED_COMPRESS_TYPE = "combined";

private:
    static const size_t DEFAULT_COMPRESS_BUFFER_SIZE;
    std::string _compressName;
    std::string _compressType;
    std::string _excludePattern;
    uint64_t _compressBufferSize;
    std::map<std::string, std::string> _compressParameters;
    std::string _statisticKey;
    std::vector<std::string> _statisticValues;
    std::vector<std::string> _compressorNames;
    std::string _defaultCompressor;

private:
    friend class indexlib::config::FileCompressConfig;
    AUTIL_LOG_DECLARE();
};

typedef std::vector<SingleFileCompressConfig> SingleFileCompressConfigVec;
} // namespace indexlibv2::config
