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
#include "ha3/config/RankProfileConfig.h"

#include <algorithm>
#include <vector>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/plugin/ModuleInfo.h"
#include "ha3/config/RankProfileInfo.h"
#include "ha3/util/TypeDefine.h"

namespace isearch {
namespace config {
AUTIL_LOG_SETUP(ha3, RankProfileConfig);

RankProfileConfig::RankProfileConfig() {
    _rankProfileInfos.push_back(RankProfileInfo());
}

RankProfileConfig::~RankProfileConfig() {
}

void RankProfileConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
    JSONIZE(json, "modules", _modules);
    JSONIZE(json, "rankprofiles", _rankProfileInfos);
}

bool RankProfileConfig::operator==(const RankProfileConfig &other) const {
    if (&other == this) {
        return true;
    }
    if (_modules.size() != other._modules.size() ||
        _rankProfileInfos.size() != other._rankProfileInfos.size())
    {
        return false;
    }
    return _modules == other._modules
        && _rankProfileInfos == other._rankProfileInfos;
}

} // namespace config
} // namespace isearch
