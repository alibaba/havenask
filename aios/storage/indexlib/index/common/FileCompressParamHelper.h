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

#include "autil/NoCopyable.h"
#include "indexlib/config/FileCompressConfig.h"
#include "indexlib/config/FileCompressConfigV2.h"
#include "indexlib/config/SingleFileCompressConfig.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/framework/SegmentStatistics.h"
#include "indexlib/index/common/data_structure/VarLenDataParam.h"

namespace indexlibv2::index {

class FileCompressParamHelper : private autil::NoCopyable
{
public:
    FileCompressParamHelper() = default;
    ~FileCompressParamHelper() = default;

public:
    static void SyncParam(const std::shared_ptr<indexlib::config::FileCompressConfig>& compressConfig,
                          const std::string& temperatureLayer, indexlib::file_system::WriterOption& option)
    {
        if (!compressConfig) {
            return;
        }
        option.compressorName = compressConfig->GetCompressType(temperatureLayer);
        option.compressBufferSize = compressConfig->GetCompressBufferSize(temperatureLayer);
        option.compressExcludePattern = compressConfig->GetExcludePattern(temperatureLayer);
        option.compressorParams = compressConfig->GetParameters(temperatureLayer);
    }

    static void SyncParam(const std::shared_ptr<config::FileCompressConfigV2>& fileCompressConfig,
                          const std::shared_ptr<framework::SegmentStatistics>& segmentStatistics,
                          indexlib::file_system::WriterOption& option)
    {
        if (!fileCompressConfig) {
            return;
        }
        auto targetCompressConfig = GetCompressConfig(fileCompressConfig, segmentStatistics);
        if (targetCompressConfig.GetCompressType().empty()) {
            return;
        }
        option.compressorName = targetCompressConfig.GetCompressType();
        option.compressBufferSize = targetCompressConfig.GetCompressBufferSize();
        option.compressExcludePattern = targetCompressConfig.GetExcludePattern();
        option.compressorParams = targetCompressConfig.GetParameters();
    }

    static void SyncParam(const std::shared_ptr<indexlib::config::FileCompressConfig>& compressConfig,
                          const std::string& temperatureLayer, VarLenDataParam& param)
    {
        if (!compressConfig) {
            return;
        }
        param.dataCompressorName = compressConfig->GetCompressType(temperatureLayer);
        param.dataCompressBufferSize = compressConfig->GetCompressBufferSize(temperatureLayer);
        param.compressWriterExcludePattern = compressConfig->GetExcludePattern(temperatureLayer);
        param.dataCompressorParams = compressConfig->GetParameters(temperatureLayer);
    }

    static void SyncParam(const std::shared_ptr<config::FileCompressConfigV2>& fileCompressConfig,
                          const std::shared_ptr<framework::SegmentStatistics>& segmentStatistics,
                          VarLenDataParam& param)
    {
        if (!fileCompressConfig) {
            return;
        }
        auto targetCompressConfig = GetCompressConfig(fileCompressConfig, segmentStatistics);
        if (targetCompressConfig.GetCompressType().empty()) {
            return;
        }
        param.dataCompressorName = targetCompressConfig.GetCompressType();
        param.dataCompressBufferSize = targetCompressConfig.GetCompressBufferSize();
        param.compressWriterExcludePattern = targetCompressConfig.GetExcludePattern();
        param.dataCompressorParams = targetCompressConfig.GetParameters();
    }

private:
    static config::SingleFileCompressConfig
    GetCompressConfig(const std::shared_ptr<config::FileCompressConfigV2>& fileCompressConfig,
                      std::shared_ptr<framework::SegmentStatistics> statistics)
    {
        auto configs = fileCompressConfig->GetConfigs();
        if (!configs) {
            return config::SingleFileCompressConfig();
        }
        if (!config::SingleFileCompressConfig::ValidateConfigs(*configs).IsOK()) {
            return config::SingleFileCompressConfig();
        }
        auto compressName = fileCompressConfig->GetCompressName();
        auto findConfigByName = [configs](const std::string& name) {
            for (const auto& config : *configs) {
                if (config.GetCompressName() == name) {
                    return config;
                }
            }
            return config::SingleFileCompressConfig();
        };
        auto config = findConfigByName(compressName);
        if (config.GetCompressType() != config::SingleFileCompressConfig::COMBINED_COMPRESS_TYPE) {
            return config;
        }

        compressName = config.GetDefaultCompressor();
        std::string value;
        if (statistics && statistics->GetStatistic(config.GetStatisticKey(), value)) {
            auto values = config.GetStatisticValues();
            auto compressors = config.GetCompressorNames();
            assert(values.size() == compressors.size());
            for (size_t i = 0; i < values.size(); ++i) {
                if (values[i] == value) {
                    compressName = compressors[i];
                }
            }
        }
        return findConfigByName(compressName);
    }
};

} // namespace indexlibv2::index
