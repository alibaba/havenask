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
#include "ha3/rank/MultiHitCollector.h"

#include <cstddef>

#include "autil/mem_pool/PoolBase.h"
#include "suez/turing/expression/common.h"

#include "ha3/common/AttributeItem.h"
#include "ha3/rank/HitCollectorBase.h"
#include "autil/Log.h"

namespace autil {
namespace mem_pool {
class Pool;
template <typename T> class PoolVector;
}  // namespace mem_pool
}  // namespace autil
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
AUTIL_LOG_SETUP(ha3, MultiHitCollector);

MultiHitCollector::MultiHitCollector(autil::mem_pool::Pool *pool,
                                     const common::Ha3MatchDocAllocatorPtr &allocatorPtr,
                                     AttributeExpression *expr)
    : HitCollectorBase(expr, pool, allocatorPtr)
{
}

MultiHitCollector::~MultiHitCollector() {
    for (size_t i = 0; i < _hitCollectors.size(); ++i) {
        POOL_DELETE_CLASS(_hitCollectors[i]);
    }
}

matchdoc::MatchDoc MultiHitCollector::collectOneDoc(matchdoc::MatchDoc matchDoc) {
    auto retDoc = matchDoc;
    for (vector<HitCollectorBase*>::iterator it = _hitCollectors.begin();
         it != _hitCollectors.end(); ++it)
    {
        retDoc = (*it)->collectOneDoc(retDoc);
        if (retDoc == matchdoc::INVALID_MATCHDOC) {
             break;
        }
    }
    return retDoc;
}

void MultiHitCollector::doStealAllMatchDocs(autil::mem_pool::PoolVector<matchdoc::MatchDoc> &target) {
    for (vector<HitCollectorBase*>::iterator it = _hitCollectors.begin();
         it != _hitCollectors.end(); ++it)
    {
        (*it)->stealAllMatchDocs(target);
    }
}

uint32_t MultiHitCollector::doGetItemCount() const {
    uint32_t itemCount = 0;
    for (vector<HitCollectorBase *>::const_iterator it = _hitCollectors.begin();
         it != _hitCollectors.end(); ++it)
    {
        itemCount += (*it)->doGetItemCount();
    }
    return itemCount;
}

matchdoc::MatchDoc MultiHitCollector::top() const {
    assert(false);
    return matchdoc::INVALID_MATCHDOC;
}

} // namespace rank
} // namespace isearch
