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

#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Types.h"

namespace indexlibv2::framework {

// use version coordinate to locate a version.
class VersionCoord : public autil::legacy::Jsonizable
{
public:
    // leave this construct function implicit conversion intentionally
    VersionCoord() = default;
    VersionCoord(versionid_t verisionId);
    explicit VersionCoord(versionid_t verisionId, const std::string& fenceName);
    ~VersionCoord() = default;

    void Jsonize(JsonWrapper& json) override;
    versionid_t GetVersionId() const { return _versionId; }
    const std::string& GetVersionFenceName() const { return _versionFenceName; }
    std::string DebugString() const;
    bool operator==(const VersionCoord& other) const;
    bool operator<(const VersionCoord& other) const;

private:
    versionid_t _versionId = INVALID_VERSIONID;
    std::string _versionFenceName;
};

} // namespace indexlibv2::framework
