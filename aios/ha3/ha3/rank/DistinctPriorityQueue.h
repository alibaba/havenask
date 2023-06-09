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

#include <assert.h>
#include <stdint.h>
#include <memory>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/rank/DistinctInfo.h"
#include "ha3/rank/MatchDocPriorityQueue.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/Reference.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil
namespace isearch {
namespace rank {
class Comparator;
}  // namespace rank
}  // namespace isearch

namespace isearch {
namespace rank {

class DistinctPriorityQueue : public MatchDocPriorityQueue
{
public:
    DistinctPriorityQueue(uint32_t size, autil::mem_pool::Pool *pool,
                          const Comparator *cmp,
                          matchdoc::Reference<DistinctInfo> *distinctInfoRef);
    ~DistinctPriorityQueue();
private:
    DistinctPriorityQueue(const DistinctPriorityQueue &);
    DistinctPriorityQueue& operator=(const DistinctPriorityQueue &);
public:
    void swap(uint32_t a, uint32_t b) override;
private:
    void setQueuePosition(matchdoc::MatchDoc matchDoc,
                          uint32_t idx) 
    {
        DistinctInfo *distInfo 
            = _distinctInfoRef->getPointer(matchDoc);
        assert(distInfo);
        distInfo->setQueuePosition(idx);
    }
private:
    matchdoc::Reference<DistinctInfo> *_distinctInfoRef;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<DistinctPriorityQueue> DistinctPriorityQueuePtr;

} // namespace rank
} // namespace isearch

