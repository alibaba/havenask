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
#include "ha3/search/SimpleMatchDataFetcher.h"

#include <stddef.h>
#include <algorithm>

#include "autil/Log.h"
#include "ha3/common/CommonDef.h"
#include "ha3/search/SimpleMatchData.h"
#include "indexlib/indexlib.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/ValueType.h"

using namespace isearch::common;

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, SimpleMatchDataFetcher);
using namespace matchdoc;

SimpleMatchDataFetcher::SimpleMatchDataFetcher()
    : _ref(NULL)
{
}

SimpleMatchDataFetcher::~SimpleMatchDataFetcher() {
}

ReferenceBase *SimpleMatchDataFetcher::require(
        MatchDocAllocator *allocator,
        const std::string &refName, uint32_t termCount)
{
    _ref = createReference<rank::SimpleMatchData>(
            allocator, refName, termCount);
    _termCount = termCount;
    return _ref;
}

indexlib::index::ErrorCode SimpleMatchDataFetcher::fillMatchData(
        const SingleLayerExecutors &singleLayerExecutors,
        MatchDoc matchDoc, MatchDoc subDoc) const
{
    docid_t docId = matchDoc.getDocId();
    rank::SimpleMatchData &data = _ref->getReference(matchDoc);
    for (uint32_t i = 0; i < _termCount; ++i) {
        int32_t idx = (int32_t)i - (int32_t)_accTermCount;
        bool isMatch = idx >= 0
                       && (uint32_t)idx < singleLayerExecutors.size()
                       && singleLayerExecutors[idx]
                       && singleLayerExecutors[idx]->getDocId() == docId;
        data.setMatch(i, isMatch);
    }
    return indexlib::index::ErrorCode::OK;
}

} // namespace search
} // namespace isearch
