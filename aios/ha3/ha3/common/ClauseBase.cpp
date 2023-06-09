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
#include "ha3/common/ClauseBase.h"

#include "ha3/util/TypeDefine.h"
#include "autil/Log.h"

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, ClauseBase);

ClauseBase::ClauseBase() {
}

ClauseBase::~ClauseBase() {
}

std::string ClauseBase::boolToString(bool flag)  {
    if (flag) {
        return std::string("true");
    } else {
        return std::string("false");
    }
}

std::string ClauseBase::cluster2ZoneName(const std::string& clusterName) {
    std::string zoneName = clusterName;
    if (clusterName.find(".") == std::string::npos) {
        zoneName += std::string(ZONE_BIZ_NAME_SPLITTER) + DEFAULT_BIZ_NAME;
    }
    return zoneName;
}


} // namespace common
} // namespace isearch
