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
#include "indexlib/index/kv/ValueWriter.h"

#include "indexlib/file_system/WriterOption.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/index/kv/config/KVIndexPreference.h"

namespace indexlibv2::index {

ValueWriter::~ValueWriter() {}

void ValueWriter::FillWriterOption(const indexlibv2::config::KVIndexConfig& config,
                                   indexlib::file_system::WriterOption& opts)
{
    const auto& valueParam = config.GetIndexPreference().GetValueParam();
    if (valueParam.EnableFileCompress()) {
        opts.compressorName = valueParam.GetFileCompressType();
        opts.compressBufferSize = valueParam.GetFileCompressBufferSize();
        opts.compressorParams = valueParam.GetFileCompressParameter();
    }
    opts.openType = indexlib::file_system::FSOT_BUFFERED;
}

} // namespace indexlibv2::index
