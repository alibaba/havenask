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
#include "indexlib/file_system/package/PackageFileTagConfig.h"

#include <iosfwd>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/file_system/load_config/LoadConfig.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/RegularExpression.h"

using namespace std;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, PackageFileTagConfig);

PackageFileTagConfig::PackageFileTagConfig() {}

PackageFileTagConfig::~PackageFileTagConfig() {}

void PackageFileTagConfig::Init()
{
    for (const auto& filePattern : _filePatterns) {
        util::RegularExpressionPtr regex(new util::RegularExpression);
        if (!regex->Init(LoadConfig::BuiltInPatternToRegexPattern(filePattern))) {
            INDEXLIB_FATAL_ERROR(BadParameter, "Invalid parameter for regular expression pattern[%s], error[%s]",
                                 filePattern.c_str(), regex->GetLatestErrMessage().c_str());
        }
        _regexVec.push_back(regex);
    }
}

void PackageFileTagConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("file_patterns", _filePatterns, _filePatterns);
    json.Jsonize("tag", _tag, _tag);
    if (json.GetMode() == FROM_JSON) {
        Init();
    }
}

bool PackageFileTagConfig::Match(const string& filePath) const
{
    for (auto& regex : _regexVec) {
        if (regex->Match(filePath)) {
            return true;
        }
    }
    return false;
}

}} // namespace indexlib::file_system
