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
#include "ha3/search/RangeQueryExecutor.h"

#include <iosfwd>

#include "autil/CommonMacros.h"
#include "autil/mem_pool/PoolVector.h"
#include "ha3/isearch.h"
#include "ha3/search/LayerMetas.h"
#include "ha3/search/QueryExecutor.h"

using namespace std;
using namespace isearch::search;

namespace isearch {
namespace search {

RangeQueryExecutor::RangeQueryExecutor(LayerMeta *layerMeta)
    : _layerMeta(*layerMeta)
    , _rangeIdx(0)
    , _rangeCount(_layerMeta.size())
    , _df(0)
{
    for (auto &rangeMeta : _layerMeta) {
        _df += rangeMeta.end - rangeMeta.begin + 1;
    }
    _hasSubDocExecutor = false;
}

RangeQueryExecutor::~RangeQueryExecutor() {}

void RangeQueryExecutor::reset() {
    _rangeIdx = 0;
}

indexlib::index::ErrorCode RangeQueryExecutor::seekSubDoc(
    docid_t docId, docid_t subDocId, docid_t subDocEnd, bool needSubMatchdata, docid_t &result) {
    if (getDocId() == docId && subDocId < subDocEnd) {
        result = subDocId;
    } else {
        result = END_DOCID;
    }
    return indexlib::index::ErrorCode::OK;
}

indexlib::index::ErrorCode RangeQueryExecutor::doSeek(docid_t id, docid_t &result) {
    if (unlikely(id == END_DOCID)) {
        result = END_DOCID;
        return indexlib::index::ErrorCode::OK;
    }
    ++_seekDocCount;
    for (; _rangeIdx < _rangeCount; ++_rangeIdx) {
        auto &range = _layerMeta[_rangeIdx];
        if (likely(id <= range.end)) {
            result = id >= range.begin ? id : range.begin;
            return indexlib::index::ErrorCode::OK;
        }
    }
    result = END_DOCID;
    return indexlib::index::ErrorCode::OK;
}

std::string RangeQueryExecutor::toString() const {
    return _layerMeta.toString();
}

bool RangeQueryExecutor::isMainDocHit(docid_t docId) const {
    return true;
}

} // namespace search
} // namespace isearch
