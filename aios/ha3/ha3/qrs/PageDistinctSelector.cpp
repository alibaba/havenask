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
#include "ha3/qrs/PageDistinctSelector.h"

#include "autil/Log.h"

namespace isearch {
namespace qrs {
AUTIL_LOG_SETUP(ha3, PageDistinctSelector);

PageDistinctSelector::PageDistinctSelector(uint32_t pgHitNum, 
        const std::string &distKey, uint32_t distCount, 
        const std::vector<bool> &sortFlags,
        const std::vector<double> &gradeThresholds)
{
    _pgHitNum = pgHitNum;
    _distKey = distKey;
    _distCount = distCount;
    _grades = gradeThresholds;
    _sortFlags = sortFlags;
}

PageDistinctSelector::~PageDistinctSelector() {
}

} // namespace qrs
} // namespace isearch

