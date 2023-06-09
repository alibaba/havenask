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
#include "indexlib/framework/VersionLine.h"

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, VersionLine);

VersionLine::VersionLine() : _parentVersion(INVALID_VERSIONID) {}

VersionLine::~VersionLine() {}

void VersionLine::Jsonize(JsonWrapper& json)
{
    json.Jsonize("key_versions", _keyVersions, _keyVersions);
    json.Jsonize("parent_version", _parentVersion, _parentVersion);
}
void VersionLine::AddCurrentVersion(const VersionCoord& keyVersion)
{
    if (!_keyVersions.empty()) {
        _parentVersion = VersionCoord(_keyVersions.back().second, _keyVersions.back().first);
    }
    if (!_keyVersions.empty() && _keyVersions.back().first == keyVersion.GetVersionFenceName()) {
        _keyVersions.back().second = keyVersion.GetVersionId();
    } else {
        _keyVersions.emplace_back(keyVersion.GetVersionFenceName(), keyVersion.GetVersionId());
    }
    if (_keyVersions.size() > KEY_NODE_COUNT) {
        decltype(_keyVersions) newKeyVersions(_keyVersions.begin() + _keyVersions.size() - KEY_NODE_COUNT,
                                              _keyVersions.end());
        _keyVersions = newKeyVersions;
    }
}

bool VersionLine::CanFastFowardFrom(const VersionCoord& keyVersion, bool isDirty) const
{
    if (keyVersion.GetVersionFenceName().empty()) {
        AUTIL_LOG(INFO, "any version can fast from empty fence");
        return true;
    }
    for (auto iter = _keyVersions.rbegin(); iter != _keyVersions.rend(); iter++) {
        auto [fence, versionId] = *iter;
        if (fence == keyVersion.GetVersionFenceName()) {
            if (isDirty) {
                return keyVersion.GetVersionId() < versionId;
            }
            return keyVersion.GetVersionId() <= versionId;
        }
    }
    AUTIL_LOG(INFO, "head versionCoord [%s] can not fast forward, key versions [%s]", keyVersion.DebugString().c_str(),
              autil::legacy::ToJsonString(_keyVersions).c_str());
    return false;
}

bool VersionLine::operator==(const VersionLine& other) const
{
    return _keyVersions == other._keyVersions && _parentVersion == other._parentVersion;
}
VersionCoord VersionLine::GetParentVersion() const { return _parentVersion; }
VersionCoord VersionLine::GetHeadVersion() const
{
    if (_keyVersions.empty()) {
        return VersionCoord(INVALID_VERSIONID);
    }
    return VersionCoord(_keyVersions.back().second, _keyVersions.back().first);
}

} // namespace indexlibv2::framework
