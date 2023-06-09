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
#include "ha3/search/LayerMetas.h"

#include <limits>

#include "autil/mem_pool/PoolVector.h"

#include "ha3/isearch.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil

using namespace std;

namespace isearch {
namespace search {

DocIdRangeMeta::DocIdRangeMeta()
    : nextBegin(0)
    , end(0)
    , quota(0)
    , begin(0)
    , ordered(OT_UNKNOWN)
{
}

DocIdRangeMeta::DocIdRangeMeta(docid_t begin_, docid_t end_, OrderedType ot, uint32_t quota_)
    : nextBegin(begin_)
    , end(end_)
    , quota(quota_)
    , begin(begin_)
    , ordered(ot)
{
}

DocIdRangeMeta::DocIdRangeMeta(const indexlib::DocIdRange& docIdRange, OrderedType ot, uint32_t quota_)
    : nextBegin(docIdRange.first)
    , end(docIdRange.second)
    , quota(quota_)
    , begin(docIdRange.first)
    , ordered(ot)
{
}

LayerMeta::LayerMeta(autil::mem_pool::Pool *pool)
    : autil::mem_pool::PoolVector<DocIdRangeMeta>(pool)
{
    quota = 0;
    maxQuota = std::numeric_limits<uint32_t>::max();
    quotaMode = QM_PER_DOC;
    needAggregate = true;
    quotaType = QT_PROPOTION;
}

std::string LayerMeta::toString() const {
    std::stringstream ss;
    ss << "(quota: " << quota
       << " maxQuota: " << maxQuota
       << " quotaMode: " << quotaMode
       << " needAggregate: " << needAggregate
       << " quotaType: " << quotaType
       << ") ";
    for (const_iterator it = begin(); it != end(); it++) {
        ss << *it << ";";
    }
    return ss.str();
}

void LayerMeta::initRangeString() {
    std::stringstream ss;
    for (const_iterator it = begin(); it != end(); it++) {
        ss << *it << ";";
    }
    this->rangeString = ss.str();
}

std::string LayerMeta::getRangeString() const {
    return rangeString;
}

std::ostream& operator << (std::ostream &os, const DocIdRangeMeta &range) {
    os << "begin: " << range.begin
       << " end: " << range.end
       << " nextBegin: " << range.nextBegin
       << " quota: " << range.quota;
    return os;
}

} // namespace search
} // namespace isearch
