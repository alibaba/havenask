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
#include "indexlib/index/inverted_index/truncate/MultiComparator.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(index, MultiComparator);

void MultiComparator::AddComparator(const std::shared_ptr<Comparator>& comp) { _comps.push_back(comp); }

bool MultiComparator::LessThan(const DocInfo* left, const DocInfo* right) const
{
    for (size_t i = 0; i < _comps.size(); ++i) {
        if (_comps[i]->LessThan(left, right)) {
            return true;
        } else if (_comps[i]->LessThan(right, left)) {
            return false;
        }
    }
    return false;
}

} // namespace indexlib::index
