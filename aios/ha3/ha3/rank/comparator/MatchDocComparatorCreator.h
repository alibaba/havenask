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

#include <vector>
#include <string>
#include "autil/Log.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil
namespace matchdoc {
class ReferenceBase;
class MatchDocAllocator;
}  // namespace matchdoc
namespace isearch {
namespace rank {
class ComboComparator;
class Comparator;
}  // namespace rank
}  // namespace isearch

namespace isearch {
namespace rank {

class MatchDocComparatorCreator
{
public:
    MatchDocComparatorCreator(autil::mem_pool::Pool *pool,
                              matchdoc::MatchDocAllocator *allocator);
    ~MatchDocComparatorCreator();
public:
    isearch::rank::ComboComparator *createComparator(const std::vector<std::string> &refNames,
            const std::vector<bool> &orders);
private:
    rank::ComboComparator *createOptimizedComparator(
            const std::string &refName, bool flag);
    rank::ComboComparator *createOptimizedComparator(
            const std::string &refName1, const std::string &refName2, bool flag1, bool flag2);
    rank::Comparator *createComparator(const std::string &refName, bool sortFlag);
    template <typename T>
    rank::ComboComparator *createComboComparatorTyped(
            matchdoc::ReferenceBase *ref, bool flag);
    template <typename T1, typename T2>
    rank::ComboComparator *createComboComparatorTyped(
            matchdoc::ReferenceBase *ref1, matchdoc::ReferenceBase *ref2,
            bool sortFlag1, bool sortFlag2);
    template <typename T>
    rank::Comparator *createComparatorTyped(matchdoc::ReferenceBase *ref, bool flag);
private:
    autil::mem_pool::Pool *_pool;
    matchdoc::MatchDocAllocator *_allocator;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace rank
} // namespace isearch
