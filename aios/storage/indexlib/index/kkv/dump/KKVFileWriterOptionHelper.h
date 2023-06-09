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
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/index/kkv/config/KKVIndexPreference.h"

namespace indexlibv2::index {
class KKVFileWriterOptionHelper
{
public:
    // TODO: 抽象一个FileCompressOption类
    static indexlib::file_system::WriterOption
    Create(const indexlib::config::KKVIndexPreference::ValueParam& param,
           size_t bufferSize = indexlib::file_system::WriterOption::DEFAULT_BUFFER_SIZE, bool asyncDump = false)
    {
        auto option = indexlib::file_system::WriterOption::Buffer();
        option.bufferSize = bufferSize;
        option.asyncDump = asyncDump;
        if (param.EnableFileCompress()) {
            option.compressorName = param.GetFileCompressType();
            option.compressBufferSize = param.GetFileCompressBufferSize();
            option.compressorParams = param.GetFileCompressParameter();
        }
        return option;
    }
};
}