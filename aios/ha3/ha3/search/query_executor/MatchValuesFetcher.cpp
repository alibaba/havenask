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
#include "ha3/search/MatchValuesFetcher.h"

#include <stddef.h>
#include <algorithm>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/CommonDef.h" // IWYU pragma: keep
#include "ha3/common/Term.h"
#include "ha3/search/MatchValues.h"
#include "ha3/search/TermMetaInfo.h"
#include "indexlib/indexlib.h"
#include "matchdoc/CommonDefine.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Reference.h"
#include "matchdoc/ValueType.h"

using namespace isearch::common;
using namespace isearch::rank;

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, MatchValuesFetcher);
using namespace matchdoc;

MatchValuesFetcher::MatchValuesFetcher()
    : _ref(NULL)
    , _termCount(0)
    , _accTermCount(0)
{
}

MatchValuesFetcher::~MatchValuesFetcher() {
}

ReferenceBase *MatchValuesFetcher::require(
        MatchDocAllocator *allocator,
        const std::string &refName, uint32_t termCount)
{
    _ref = createReference(
            allocator, refName, termCount);
    _termCount = termCount;
    return _ref;
}

matchdoc::Reference<Ha3MatchValues> *MatchValuesFetcher::createReference(
            matchdoc::MatchDocAllocator *allocator,
            const std::string &refName, uint32_t termCount)
{
    return allocator->declare<Ha3MatchValues>(
            refName, isearch::common::HA3_MATCHVALUE_GROUP, SL_NONE,
            Ha3MatchValues::sizeOf(termCount));
}

indexlib::index::ErrorCode MatchValuesFetcher::fillMatchValues(
        const SingleLayerExecutors &singleLayerExecutors,
        MatchDoc matchDoc)
{
    docid_t docId = matchDoc.getDocId();
    Ha3MatchValues &mValues = _ref->getReference(matchDoc);
    mValues.setMaxNumTerms(_termCount);
    for (uint32_t i = 0; i < _termCount; ++i) {
        matchvalue_t &mvt = mValues.nextFreeMatchValue();
        mvt = matchvalue_t();
        int32_t idx = (int32_t)i - (int32_t)_accTermCount;
        if (idx >= 0
            && (uint32_t)idx < singleLayerExecutors.size()
            && singleLayerExecutors[idx]
            && singleLayerExecutors[idx]->getDocId() == docId)
        {
            mvt = singleLayerExecutors[idx]->getMatchValue();
        }
    }
    return indexlib::index::ErrorCode::OK;
}

} // namespace search
} // namespace isearch
