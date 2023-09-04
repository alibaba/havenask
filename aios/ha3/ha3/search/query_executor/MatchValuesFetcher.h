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

#include <memory>
#include <stdint.h>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/search/MatchValues.h"
#include "ha3/search/QueryExecutor.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/misc/common.h"

namespace matchdoc {
class MatchDoc;
class MatchDocAllocator;
class ReferenceBase;
template <typename T>
class Reference;
} // namespace matchdoc

namespace isearch {
namespace search {

class MatchValuesFetcher {
public:
    MatchValuesFetcher();
    ~MatchValuesFetcher();

private:
    MatchValuesFetcher(const MatchValuesFetcher &);
    MatchValuesFetcher &operator=(const MatchValuesFetcher &);

public:
    void setAccTermCount(uint32_t accTermCount) {
        _accTermCount = accTermCount;
    }
    matchdoc::ReferenceBase *
    require(matchdoc::MatchDocAllocator *allocator, const std::string &refName, uint32_t termCount);
    matchdoc::Reference<rank::MatchValues> *createReference(matchdoc::MatchDocAllocator *allocator,
                                                            const std::string &refName,
                                                            uint32_t termCount);
    indexlib::index::ErrorCode fillMatchValues(const SingleLayerExecutors &singleLayerExecutors,
                                               matchdoc::MatchDoc);

private:
    matchdoc::Reference<rank::MatchValues> *_ref;
    uint32_t _termCount;
    uint32_t _accTermCount;

private:
    friend class MatchValuesFetcherTest;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<MatchValuesFetcher> MatchValuesFetcherPtr;

} // namespace search
} // namespace isearch
