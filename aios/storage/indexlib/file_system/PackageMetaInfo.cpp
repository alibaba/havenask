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
#include "indexlib/file_system/PackageMetaInfo.h"

#include <sstream>
#include <utility>

using namespace std;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, PackageMetaInfo);

void PackageMetaInfo::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("length", length, length);
    if (json.GetMode() == FROM_JSON) {
        json.Jsonize("files", files, files);
    } else {
        if (!files.empty()) {
            json.Jsonize("files", files, files);
        }
    }
}

string PackageMetaInfo::DebugString() const
{
    stringstream ss;
    ss << "length: " << length << endl;
    ss << "files: " << endl;
    for (const auto& pair : files) {
        ss << "path: " << pair.first << " meta: " << pair.second.DebugString() << endl;
    }
    return ss.str();
}
}} // namespace indexlib::file_system
