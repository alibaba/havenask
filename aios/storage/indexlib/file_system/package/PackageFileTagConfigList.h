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
#include "indexlib/file_system/package/PackageFileTagConfig.h"

namespace indexlib { namespace file_system {

class PackageFileTagConfigList : public autil::legacy::Jsonizable
{
public:
    PackageFileTagConfigList();
    ~PackageFileTagConfigList();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    const std::string& Match(const std::string& relativeFilePath, const std::string& defaultTag) const;

public:
    void TEST_PATCH();

private:
    std::vector<PackageFileTagConfig> configs;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<PackageFileTagConfigList> PackageFileTagConfigListPtr;
}} // namespace indexlib::file_system
