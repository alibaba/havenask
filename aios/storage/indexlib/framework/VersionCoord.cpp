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
#include "indexlib/framework/VersionCoord.h"

#include "autil/StringUtil.h"

namespace indexlibv2::framework {

VersionCoord::VersionCoord(versionid_t versionId) : _versionId(versionId) {}
void VersionCoord::Jsonize(JsonWrapper& json)
{
    json.Jsonize("version_id", _versionId, _versionId);
    json.Jsonize("version_fence_name", _versionFenceName, _versionFenceName);
}

VersionCoord::VersionCoord(versionid_t versionId, const std::string& versionFenceName)
    : _versionId(versionId)
    , _versionFenceName(versionFenceName)
{
}

std::string VersionCoord::DebugString() const
{
    return autil::StringUtil::toString(_versionId) + ":" + _versionFenceName;
}

bool VersionCoord::operator==(const VersionCoord& other) const
{
    return _versionId == other._versionId && _versionFenceName == other._versionFenceName;
}

bool VersionCoord::operator<(const VersionCoord& other) const
{
    return (_versionId == other._versionId) ? _versionFenceName < other._versionFenceName
                                            : _versionId < other._versionId;
}

} // namespace indexlibv2::framework
