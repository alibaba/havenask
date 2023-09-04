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

#include <optional>
#include <string>

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/config/MutableJson.h"

namespace indexlibv2::config {
class FieldConfig;
class FileCompressConfigV2;
class SingleFileCompressConfig;

class IndexConfigDeserializeResource : private autil::NoCopyable
{
public:
    IndexConfigDeserializeResource(const std::vector<std::shared_ptr<FieldConfig>>& fieldConfigs,
                                   MutableJson& runtimeSettings);

public:
    const std::shared_ptr<FieldConfig>& GetFieldConfig(const std::string& fieldName) const;
    template <typename T>
    std::optional<T> GetRuntimeSetting(const std::string& path) const;

    std::shared_ptr<config::FileCompressConfigV2> GetFileCompressConfig(const std::string& compressName) const;

private:
    std::map</*fieldName*/ std::string, std::shared_ptr<FieldConfig>> _fieldConfigs;
    mutable autil::legacy::json::JsonMap _settings;
    MutableJson& _runtimeSettings;
    mutable std::shared_ptr<std::vector<config::SingleFileCompressConfig>> _fileCompressVec;

private:
    AUTIL_LOG_DECLARE();
};

///////////////////////////////////////////////

template <typename T>
inline std::optional<T> IndexConfigDeserializeResource::GetRuntimeSetting(const std::string& path) const
{
    auto [status, object] = _runtimeSettings.GetValue<T>(path);
    if (status.IsOK()) {
        return object;
    } else {
        return std::nullopt;
    }
}

} // namespace indexlibv2::config
