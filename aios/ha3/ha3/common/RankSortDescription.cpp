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
#include "ha3/common/RankSortDescription.h"

#include "autil/DataBuffer.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "ha3/common/SortDescription.h"

using namespace autil;
namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, RankSortDescription);

RankSortDescription::RankSortDescription() {
    _percent = 100.0;
}

RankSortDescription::~RankSortDescription() {
    for (size_t i = 0; i < _sortDescs.size(); ++i) {
        delete _sortDescs[i];
    }
    _sortDescs.clear();
}

void RankSortDescription::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_sortDescs);
    dataBuffer.write(_percent);
}

void RankSortDescription::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(_sortDescs);
    dataBuffer.read(_percent);
}

void RankSortDescription::setSortDescs(const std::vector<SortDescription*> &sortDesc) {
    _sortDescs = sortDesc;
}

void RankSortDescription::setPercent(const std::string &percentStr) {
    _percent = autil::StringUtil::fromString<float>(percentStr);
}

std::string RankSortDescription::toString() const {
    std::string descStr;
    for (size_t i = 0; i < _sortDescs.size(); i++) {
        assert(_sortDescs[i]);
        descStr.append(_sortDescs[i]->toString());
        descStr.append("#");
    }
    descStr.append(",percent:");
    descStr.append(StringUtil::toString(_percent));
    return descStr;
}

} // namespace common
} // namespace isearch

