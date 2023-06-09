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
#include "indexlib/index/aggregation/MultiSegmentKeyIterator.h"

#include <algorithm>

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, MultiSegmentKeyIterator);

void MultiSegmentKeyIterator::Init(std::vector<std::unique_ptr<IKeyIterator>> iters)
{
    _keys.reserve(iters.size());
    for (const auto& iter : iters) {
        while (iter->HasNext()) {
            _keys.push_back(iter->Next());
        }
    }
    std::sort(_keys.begin(), _keys.end());
    auto it = std::unique(_keys.begin(), _keys.end());
    _keys.erase(it, _keys.end());
}

bool MultiSegmentKeyIterator::HasNext() const { return _cursor < _keys.size(); }

uint64_t MultiSegmentKeyIterator::Next() { return _keys[_cursor++]; }

} // namespace indexlibv2::index
