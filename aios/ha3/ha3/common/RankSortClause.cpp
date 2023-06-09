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
#include "ha3/common/RankSortClause.h"

#include "autil/DataBuffer.h"

#include "ha3/common/RankSortDescription.h"
#include "autil/Log.h"

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, RankSortClause);

RankSortClause::RankSortClause() {
}

RankSortClause::~RankSortClause() {
    for (size_t i = 0; i < _descs.size(); ++i) {
        delete _descs[i];
    }
    _descs.clear();
}

void RankSortClause::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_originalString);
    dataBuffer.write(_descs);
}

void RankSortClause::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(_originalString);
    dataBuffer.read(_descs);
}

std::string RankSortClause::toString() const {
    std::string rankSortStr;
    for (size_t i = 0; i < _descs.size(); i++) {
        assert(_descs[i]);
        rankSortStr.append(_descs[i]->toString());
        rankSortStr.append(";");
    }
    return rankSortStr;
}

} // namespace common
} // namespace isearch

