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

#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/util/RegularExpression.h"

namespace indexlib { namespace file_system {

class PackageFileTagConfig : public autil::legacy::Jsonizable
{
public:
    PackageFileTagConfig();
    ~PackageFileTagConfig();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool Match(const std::string& filePath) const;
    const std::string& GetTag() const { return _tag; }

private:
    void Init();

private:
    util::RegularExpressionVector _regexVec;
    std::vector<std::string> _filePatterns;
    std::string _tag;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<PackageFileTagConfig> PackageFileTagConfigPtr;
}} // namespace indexlib::file_system
