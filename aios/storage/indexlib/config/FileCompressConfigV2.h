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
#include <string>
#include <vector>

#include "autil/Log.h"
#include "indexlib/config/SingleFileCompressConfig.h"

namespace indexlibv2::config {

class FileCompressConfigV2
{
public:
    FileCompressConfigV2(const std::shared_ptr<std::vector<SingleFileCompressConfig>>& configs,
                         const std::string& compressName);
    ~FileCompressConfigV2() = default;

    const std::shared_ptr<std::vector<SingleFileCompressConfig>>& GetConfigs() const;
    const std::string& GetCompressName() const;

private:
    std::shared_ptr<std::vector<SingleFileCompressConfig>> _configs;
    std::string _compressName;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::config
