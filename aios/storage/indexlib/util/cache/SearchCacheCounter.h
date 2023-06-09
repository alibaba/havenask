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

namespace indexlib { namespace util {

struct SearchCacheCounter {
    int64_t hitCount = 0;
    int64_t missCount = 0;
    void Reset()
    {
        hitCount = 0;
        missCount = 0;
    }
    SearchCacheCounter& operator+=(const SearchCacheCounter& other)
    {
        hitCount += other.hitCount;
        missCount += other.missCount;
        return *this;
    }
};
}} // namespace indexlib::util
