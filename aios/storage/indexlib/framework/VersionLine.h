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

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/framework/VersionCoord.h"

namespace indexlibv2::framework {
/*
    [fence a, version 1]
            |
    [fence a, version 2]
            |
    [fence a, version 3]
            |            \
    [fence a, version 4]  [fence b, version 6]
            |                     |             \
    [fence a, version 5]  [fence b, version 7]  [fence c, version 8]
           |                                           |
    [fence a, version 10]                      [fence c, version 9]
    用来表明这个version的历史信息，判断两个version是否能用来直接reopen，是否是在一条链上的
    每个version会记录若干个分叉结点，如version 9会记录  {[fence a, version 3], [fence b, version 6], [fence c, version
   9]} 最后一个为当前version 判断version 2与version 9是否在一条分支上的逻辑为： 0.判断version 2与version 9
    是否是同一fence， 若是则根据version id大小比较即可 1.在version 9的历史中找到version 1的fence
    a，对应的关键结点为version 3 2.判断version 1 <= version 3，为一条链上的。若为version 4则无法reopen到version 9
*/

class VersionLine : public autil::legacy::Jsonizable
{
public:
    VersionLine();
    ~VersionLine();

public:
    void Jsonize(JsonWrapper& json) override;
    void AddCurrentVersion(const VersionCoord& keyVersion);
    bool CanFastFowardFrom(const VersionCoord& keyVersion, bool isDirty) const;
    VersionCoord GetParentVersion() const;
    VersionCoord GetHeadVersion() const;
    bool operator==(const VersionLine& other) const;

private:
    static constexpr uint32_t KEY_NODE_COUNT = 10;
    VersionCoord _parentVersion;
    //<fence name, version id>
    std::vector<std::pair<std::string, versionid_t>> _keyVersions;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
