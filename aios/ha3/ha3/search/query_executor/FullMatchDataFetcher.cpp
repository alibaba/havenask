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
#include "ha3/search/FullMatchDataFetcher.h"

#include <algorithm>
#include <stddef.h>

#include "autil/Log.h"
#include "ha3/common/CommonDef.h"
#include "ha3/search/MatchData.h"
#include "ha3/search/TermMatchData.h"
#include "ha3/search/TermMetaInfo.h"
#include "indexlib/indexlib.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/Reference.h"
#include "matchdoc/ValueType.h"

namespace matchdoc {
class MatchDocAllocator;
} // namespace matchdoc

using namespace isearch::common;
using namespace isearch::rank;

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, FullMatchDataFetcher);
using namespace matchdoc;

FullMatchDataFetcher::FullMatchDataFetcher()
    : _ref(NULL) {}

FullMatchDataFetcher::~FullMatchDataFetcher() {}

ReferenceBase *FullMatchDataFetcher::require(MatchDocAllocator *allocator,
                                             const std::string &refName,
                                             uint32_t termCount) {
    _ref = createReference<MatchData>(allocator, refName, termCount);
    _termCount = termCount;
    return _ref;
}

indexlib::index::ErrorCode FullMatchDataFetcher::fillMatchData(
    const SingleLayerExecutors &singleLayerExecutors, MatchDoc matchDoc, MatchDoc subDoc) const {
    docid_t docId = matchDoc.getDocId();
    MatchData &data = _ref->getReference(matchDoc);
    data.setMaxNumTerms(_termCount);
    for (uint32_t i = 0; i < _termCount; ++i) {
        TermMatchData &tmd = data.nextFreeMatchData();
        int32_t idx = (int32_t)i - (int32_t)_accTermCount;
        if (idx >= 0 && (uint32_t)idx < singleLayerExecutors.size() && singleLayerExecutors[idx]
            && singleLayerExecutors[idx]->getDocId() == docId) {
            auto ec = singleLayerExecutors[idx]->unpackMatchData(tmd);
            IE_RETURN_CODE_IF_ERROR(ec);
        }
    }
    return indexlib::index::ErrorCode::OK;
}

} // namespace search
} // namespace isearch
