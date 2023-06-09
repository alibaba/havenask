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
#include "ha3/rank/HitCollector.h"

#include <iosfwd>
#include <memory>

#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "autil/mem_pool/PoolVector.h"
#include "ha3/common/AttributeItem.h"
#include "ha3/rank/ComboComparator.h"
#include "ha3/rank/HitCollectorBase.h"
#include "ha3/rank/MatchDocPriorityQueue.h"
#include "matchdoc/MatchDoc.h"
#include "suez/turing/expression/common.h"

namespace suez {
namespace turing {
class AttributeExpression;
}  // namespace turing
}  // namespace suez

using namespace std;
using namespace suez::turing;

using namespace isearch::common;

namespace isearch {
namespace rank {
AUTIL_LOG_SETUP(ha3, HitCollector);


HitCollector::HitCollector(uint32_t size,
                           autil::mem_pool::Pool *pool,
                           const common::Ha3MatchDocAllocatorPtr &allocatorPtr,
                           AttributeExpression *expr,
                           ComboComparator *cmp)
    : HitCollectorBase(expr, pool, allocatorPtr)
{
    _queue = POOL_NEW_CLASS(pool, MatchDocPriorityQueue,
                            size, pool, cmp);
    _cmp = cmp;
    addExtraDocIdentifierCmp(_cmp);
}

HitCollector::~HitCollector() {
    uint32_t count = _queue->count();
    auto leftMatchDocs = _queue->getAllMatchDocs();
    for (uint32_t i = 0; i < count; ++i) {
        _matchDocAllocatorPtr->deallocate(leftMatchDocs[i]);
    }
    POOL_DELETE_CLASS(_queue);
    POOL_DELETE_CLASS(_cmp);
}

matchdoc::MatchDoc HitCollector::collectOneDoc(matchdoc::MatchDoc matchDoc) {
    auto retDoc = matchdoc::INVALID_MATCHDOC;
    _queue->push(matchDoc, &retDoc);
    return retDoc;
}

void HitCollector::doQuickInit(matchdoc::MatchDoc *matchDocs, uint32_t count) {
    _queue->quickInit(matchDocs, count);
}

void HitCollector::doStealAllMatchDocs(autil::mem_pool::PoolVector<matchdoc::MatchDoc> &target) {
    auto matchDocs = _queue->getAllMatchDocs();
    uint32_t c = _queue->count();
    target.insert(target.end(), matchDocs, matchDocs + c);
    _queue->reset();
}

} // namespace rank
} // namespace isearch

