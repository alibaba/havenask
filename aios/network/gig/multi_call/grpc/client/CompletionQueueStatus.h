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
#ifndef ISEARCH_MULTI_CALL_COMPLETIONQUEUESTATUS_H
#define ISEARCH_MULTI_CALL_COMPLETIONQUEUESTATUS_H

#include <grpc++/completion_queue.h>

#include "aios/network/gig/multi_call/common/common.h"
#include "autil/Lock.h"

namespace multi_call {

typedef std::shared_ptr<grpc::CompletionQueue> CompletionQueuePtr;

struct CompletionQueueStatus {
    CompletionQueueStatus(const CompletionQueuePtr &cq_) : stopped(false), cq(cq_) {
    }
    volatile bool stopped;
    autil::ReadWriteLock enqueueLock;
    CompletionQueuePtr cq;
};

MULTI_CALL_TYPEDEF_PTR(CompletionQueueStatus);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_COMPLETIONQUEUESTATUS_H
