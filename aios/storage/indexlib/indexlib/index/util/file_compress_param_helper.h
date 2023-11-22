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

#include "indexlib/common_define.h"
#include "indexlib/config/FileCompressConfig.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/index/data_structure/var_len_data_param.h"

namespace indexlib { namespace index {

class FileCompressParamHelper
{
public:
    FileCompressParamHelper();
    ~FileCompressParamHelper();

    FileCompressParamHelper(const FileCompressParamHelper&) = delete;
    FileCompressParamHelper& operator=(const FileCompressParamHelper&) = delete;
    FileCompressParamHelper(FileCompressParamHelper&&) = delete;
    FileCompressParamHelper& operator=(FileCompressParamHelper&&) = delete;

public:
    static void SyncParam(const std::shared_ptr<config::FileCompressConfig>& compressConfig,
                          const std::string& temperatureLayer, file_system::WriterOption& option)
    {
        if (!compressConfig) {
            return;
        }
        option.compressorName = compressConfig->GetCompressType(temperatureLayer);
        option.compressBufferSize = compressConfig->GetCompressBufferSize(temperatureLayer);
        option.compressExcludePattern = compressConfig->GetExcludePattern(temperatureLayer);
        option.compressorParams = compressConfig->GetParameters(temperatureLayer);
    }

    static void SyncParam(const std::shared_ptr<config::FileCompressConfig>& compressConfig,
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

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(FileCompressParamHelper);
}} // namespace indexlib::index
