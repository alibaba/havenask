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
#include "indexlib/config/FileCompressConfigV2.h"

namespace indexlibv2::config {
AUTIL_LOG_SETUP(indexlib.config, FileCompressConfigV2);

FileCompressConfigV2::FileCompressConfigV2(const std::shared_ptr<std::vector<SingleFileCompressConfig>>& configs,
                                           const std::string& compressName)
    : _configs(configs)
    , _compressName(compressName)
{
}

const std::shared_ptr<std::vector<SingleFileCompressConfig>>& FileCompressConfigV2::GetConfigs() const
{
    return _configs;
}

const std::string& FileCompressConfigV2::GetCompressName() const { return _compressName; }

} // namespace indexlibv2::config
