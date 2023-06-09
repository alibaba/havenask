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

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Status.h"

namespace indexlib::config {

class FileCompressConfig;

class FileCompressSchema : public autil::legacy::Jsonizable
{
public:
    FileCompressSchema();
    ~FileCompressSchema();

    FileCompressSchema(const FileCompressSchema&) = delete;
    FileCompressSchema& operator=(const FileCompressSchema&) = delete;
    FileCompressSchema(FileCompressSchema&&) = delete;
    FileCompressSchema& operator=(FileCompressSchema&&) = delete;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

public:
    std::shared_ptr<FileCompressConfig> GetFileCompressConfig(const std::string& name) const;
    std::vector<std::shared_ptr<FileCompressConfig>> GetFileCompressConfigs() const;
    Status CheckEqual(const FileCompressSchema& other) const;
    bool operator==(const FileCompressSchema& other) const;
    bool operator!=(const FileCompressSchema& other) const { return !(*this == other); }
    void Check();

    uint64_t GetFingerPrint() const;
    void ResetFingerPrint();
    static FileCompressSchema* FromJson(const autil::legacy::Any& any);

    // may throw exception
    void AddFileCompressConfig(const std::shared_ptr<FileCompressConfig>& config);

public:
    static constexpr uint64_t INVALID_FINGER_PRINT = 0;

private:
    std::map<std::string, std::shared_ptr<FileCompressConfig>> _fileCompressConfigMap;
    mutable uint64_t _fingerPrint;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::config
