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
#include "indexlib/file_system/package/PackageFileTagConfigList.h"

#include <algorithm>
#include <iosfwd>

using namespace std;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, PackageFileTagConfigList);

PackageFileTagConfigList::PackageFileTagConfigList() {}

PackageFileTagConfigList::~PackageFileTagConfigList() {}

void PackageFileTagConfigList::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("package_file_tag_config", configs, configs);
}

void PackageFileTagConfigList::TEST_PATCH()
{
    // eg. segment_189_level_0/attribute/sku_default_c2c/187_155.patch
    string jsonStr = R"(
    {
        "package_file_tag_config":
        [
            {"file_patterns": ["_PATCH_"], "tag": "PATCH" }
        ]
    }
    )";

    FromJsonString(*this, jsonStr);
}

const string& PackageFileTagConfigList::Match(const string& relativeFilePath, const string& defaultTag) const
{
    for (const auto& config : configs) {
        if (config.Match(relativeFilePath)) {
            return config.GetTag();
        }
    }
    return defaultTag;
}
}} // namespace indexlib::file_system
