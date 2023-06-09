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

#include <stdint.h>
#include <memory>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/Ha3MatchDocAllocator.h"
#include "ha3/rank/HitCollectorBase.h"
#include "ha3/rank/MatchDocPriorityQueue.h"
#include "matchdoc/MatchDoc.h"

namespace autil {
namespace mem_pool {
class Pool;
template <typename T> class PoolVector;
}  // namespace mem_pool
}  // namespace autil
namespace isearch {
namespace rank {
class ComboComparator;
}  // namespace rank
}  // namespace isearch
namespace suez {
namespace turing {
class AttributeExpression;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace rank {

class HitCollector : public HitCollectorBase
{
public:
    HitCollector(uint32_t size,
                 autil::mem_pool::Pool *pool,
                 const common::Ha3MatchDocAllocatorPtr &allocatorPtr,
                 suez::turing::AttributeExpression *expr,
                 ComboComparator *cmp);
    virtual ~HitCollector();
public:
    uint32_t doGetItemCount() const override {
        return _queue->count();
    }
    HitCollectorType getType() const override {
        return HCT_SINGLE;
    }
    matchdoc::MatchDoc top() const override {
        return _queue->top();
    }
    const ComboComparator *getComparator() const override {
        return _cmp;
    }
protected:
    matchdoc::MatchDoc collectOneDoc(matchdoc::MatchDoc matchDoc) override;
    void doQuickInit(matchdoc::MatchDoc *matchDocs, uint32_t count) override;
    void doStealAllMatchDocs(autil::mem_pool::PoolVector<matchdoc::MatchDoc> &target) override;
protected:
    MatchDocPriorityQueue *_queue;
    ComboComparator *_cmp;
private:
    friend class HitCollectorTest;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<HitCollector> HitCollectorPtr;

} // namespace rank
} // namespace isearch

