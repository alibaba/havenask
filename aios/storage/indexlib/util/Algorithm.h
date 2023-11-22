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
#include "autil/NoCopyable.h"

namespace indexlib::util {

class Algorithm : private autil::NoCopyable
{
public:
    Algorithm() {}
    ~Algorithm() {}

public:
    template <typename Iter>
    [[nodiscard]] static Iter SortAndUnique(Iter begin, Iter end);

    template <typename T>
    static void SortUniqueAndErase(std::vector<T>& items);

    template <typename Map, typename Keys>
    static void GetSortedKeys(const Map& hashMap, Keys* sortedKeys);

private:
    AUTIL_LOG_DECLARE();
};

template <typename T>
void Algorithm::SortUniqueAndErase(std::vector<T>& items)
{
    items.erase(SortAndUnique(items.begin(), items.end()), items.end());
}

template <typename Iter>
Iter Algorithm::SortAndUnique(Iter begin, Iter end)
{
    if (begin >= end) {
        return end;
    }
    std::stable_sort(begin, end);
    Iter curW = begin;
    Iter curR = begin;
    Iter lastDoc = end;
    for (;;) {
        if (lastDoc != end and (curR == end or *curR != *lastDoc)) {
            if (lastDoc != end) {
                *curW = *lastDoc;
                ++curW;
            }
            if (curR == end) {
                break;
            }
        }
        lastDoc = curR;
        ++curR;
    }

    return curW;
}

template <typename Map, typename Keys>
void Algorithm::GetSortedKeys(const Map& hashMap, Keys* sortedKeys)
{
    if (hashMap.empty()) {
        return;
    }

    sortedKeys->reserve(hashMap.size());
    for (auto it = hashMap.begin(); it != hashMap.end(); ++it) {
        sortedKeys->push_back(it->first);
    }
    std::sort(sortedKeys->begin(), sortedKeys->end());
    return;
}

} // namespace indexlib::util
