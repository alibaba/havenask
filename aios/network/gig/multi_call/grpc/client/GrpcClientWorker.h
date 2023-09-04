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
#ifndef ISEARCH_MULTI_CALL_GRPCCLIENTWORKER_H
#define ISEARCH_MULTI_CALL_GRPCCLIENTWORKER_H

#include <grpc++/alarm.h>
#include <grpc++/grpc++.h>

#include "aios/network/gig/multi_call/grpc/CompletionQueueStatus.h"
#include "aios/network/gig/multi_call/util/RandomGenerator.h"
#include "autil/Thread.h"

namespace multi_call {

class GrpcClientWorker
{
public:
    GrpcClientWorker(size_t threadNum);
    ~GrpcClientWorker();

private:
    GrpcClientWorker(const GrpcClientWorker &);
    GrpcClientWorker &operator=(const GrpcClientWorker &);

public:
    void stop();
    const CompletionQueueStatusPtr &getCompletionQueue(size_t allocId);

private:
    void workLoop(size_t idx, CompletionQueueStatusPtr cqsPtr);

private:
    std::vector<::grpc::Alarm *> _shutdownAlarms;
    std::vector<autil::ThreadPtr> _workThreads;
    std::vector<CompletionQueueStatusPtr> _completionQueues;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(GrpcClientWorker);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GRPCCLIENTWORKER_H
