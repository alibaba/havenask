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
#include "indexlib/config/FileCompressSchema.h"

#include <algorithm>
#include <assert.h>
#include <iosfwd>
#include <set>
#include <utility>

#include "autil/Span.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "indexlib/base/Constant.h"
#include "indexlib/config/ConfigDefine.h"
#include "indexlib/config/FileCompressConfig.h"
#include "indexlib/index/common/Types.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/KeyHasherTyped.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace indexlib::config {

AUTIL_LOG_SETUP(indexlib.config, FileCompressSchema);

FileCompressSchema::FileCompressSchema() : _fingerPrint(INVALID_FINGER_PRINT) {}

FileCompressSchema::~FileCompressSchema() {}

void FileCompressSchema::AddFileCompressConfig(const std::shared_ptr<FileCompressConfig>& config)
{
    assert(config);
    string compressName = config->GetCompressName();
    if (_fileCompressConfigMap.find(compressName) != _fileCompressConfigMap.end()) {
        INDEXLIB_FATAL_ERROR(Schema, "duplicated compress name [%s]", compressName.c_str());
    }
    config->SetSchema(this);
    _fileCompressConfigMap[compressName] = config;
    _fingerPrint = INVALID_FINGER_PRINT;
}

std::shared_ptr<FileCompressConfig> FileCompressSchema::GetFileCompressConfig(const std::string& name) const
{
    auto iter = _fileCompressConfigMap.find(name);
    if (iter == _fileCompressConfigMap.end()) {
        return std::shared_ptr<FileCompressConfig>();
    }
    return iter->second;
}

std::vector<std::shared_ptr<FileCompressConfig>> FileCompressSchema::GetFileCompressConfigs() const
{
    std::vector<std::shared_ptr<FileCompressConfig>> result;
    result.reserve(_fileCompressConfigMap.size());
    for (auto iter = _fileCompressConfigMap.begin(); iter != _fileCompressConfigMap.end(); ++iter) {
        result.push_back(iter->second);
    }
    return result;
}

void FileCompressSchema::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    if (json.GetMode() == TO_JSON) {
        vector<Any> anyVec;
        anyVec.reserve(_fileCompressConfigMap.size());
        for (const auto& iter : _fileCompressConfigMap) {
            anyVec.push_back(ToJson(iter.second));
        }
        json.Jsonize(FILE_COMPRESS, anyVec);
    }
}

Status FileCompressSchema::CheckEqual(const FileCompressSchema& other) const
{
    CHECK_CONFIG_EQUAL(_fileCompressConfigMap.size(), other._fileCompressConfigMap.size(),
                       "file compress size not equal");
    for (auto iter = _fileCompressConfigMap.begin(), iter2 = other._fileCompressConfigMap.begin();
         iter != _fileCompressConfigMap.end(); iter++, iter2++) {
        auto status = iter->second->CheckEqual(*(iter2->second));
        RETURN_IF_STATUS_ERROR(status, "file compress schema not equal");
    }
    return Status::OK();
}

void FileCompressSchema::Check()
{
    set<string> temperatureFileCompressorSet;
    for (auto iter = _fileCompressConfigMap.begin(); iter != _fileCompressConfigMap.end(); iter++) {
        vector<string> compressNames = iter->second->GetTemperatureFileCompressorNames();
        for (auto name : compressNames) {
            temperatureFileCompressorSet.insert(name);
        }
    }

    for (auto iter = temperatureFileCompressorSet.begin(); iter != temperatureFileCompressorSet.end(); iter++) {
        auto config = GetFileCompressConfig(*iter);
        if (!config) {
            INDEXLIB_FATAL_ERROR(Schema, "temperature related compressor [%s] not exist.", (*iter).c_str());
        }
        if (!config->GetTemperatureFileCompressorNames().empty()) {
            INDEXLIB_FATAL_ERROR(Schema,
                                 "temperature related compressor [%s] should not have inner temperature_compressor.",
                                 (*iter).c_str());
        }
    }
}

bool FileCompressSchema::operator==(const FileCompressSchema& other) const { return CheckEqual(other).IsOK(); }

uint64_t FileCompressSchema::GetFingerPrint() const
{
    if (_fingerPrint == INVALID_FINGER_PRINT) {
        string tmp = ToJsonString(*this, true);
        uint64_t tmpHash;
        indexlib::util::GetHashKey(indexlib::hft_murmur, autil::StringView(tmp), tmpHash);
        assert(tmpHash != INVALID_FINGER_PRINT);
        _fingerPrint = tmpHash;
    }
    return _fingerPrint;
}

FileCompressSchema* FileCompressSchema::FromJson(const Any& any)
{
    std::unique_ptr<FileCompressSchema> ret(nullptr);
    JsonArray fileCompresses = AnyCast<JsonArray>(any);
    for (JsonArray::iterator it = fileCompresses.begin(); it != fileCompresses.end(); ++it) {
        JsonMap jsonMap = AnyCast<JsonMap>(*it);
        std::shared_ptr<FileCompressConfig> config(new FileCompressConfig);
        Jsonizable::JsonWrapper jsonWrapper(jsonMap);
        config->Jsonize(jsonWrapper);
        if (!ret) {
            ret.reset(new FileCompressSchema);
        }
        ret->AddFileCompressConfig(config);
    }
    return ret.release();
}

void FileCompressSchema::ResetFingerPrint() { _fingerPrint = INVALID_FINGER_PRINT; }

} // namespace indexlib::config
