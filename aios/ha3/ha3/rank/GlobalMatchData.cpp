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
#include "ha3/rank/GlobalMatchData.h"

#include "autil/Log.h"

namespace isearch {
namespace rank {
AUTIL_LOG_SETUP(ha3, GlobalMatchData);

GlobalMatchData::GlobalMatchData(int32_t docCount) {
    _docCount = docCount;
}

GlobalMatchData::~GlobalMatchData() { 
}

int32_t GlobalMatchData::getDocCount() const {
    return _docCount;
}

void GlobalMatchData::setDocCount(int32_t docCount) {
    _docCount = docCount;
}

} // namespace rank
} // namespace isearch

