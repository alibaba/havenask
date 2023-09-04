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
#ifndef ISEARCH_CALLDELEGATIONTHREAD_H
#define ISEARCH_CALLDELEGATIONTHREAD_H

#include "aios/network/gig/multi_call/metric/WorkerMetricReporter.h"
#include "aios/network/gig/multi_call/service/CallDelegationWorkItem.h"
#include "aios/network/gig/multi_call/service/SearchServiceManager.h"
#include "autil/Lock.h"
#include "autil/LoopThread.h"

namespace multi_call {

class CallDelegationThread
{
public:
    CallDelegationThread(const SearchServiceManagerPtr &searchServiceManagerPtr,
                         const WorkerMetricReporterPtr &metricReporter,
                         int64_t processInterval = PROCESS_INTERVAL,
                         uint64_t queueSize = std::numeric_limits<uint64_t>::max());
    ~CallDelegationThread();

public:
    typedef std::deque<CallDelegationWorkItem *> CallDelegationQueue;

private:
    CallDelegationThread(const CallDelegationThread &);
    CallDelegationThread &operator=(const CallDelegationThread &);

public:
    bool start();
    void stop();
    bool isStarted() const {
        return _thread != NULL;
    }
    void pushWorkItem(CallDelegationWorkItem *workItem);
    int64_t getProcessInterval() const {
        return _processInterval;
    }

private:
    void workLoop();
    void mergeQueue();
    bool isAllQueueEmpty() const;

private:
    CallDelegationQueue _queue;
    CallDelegationQueue _pushQueue;
    autil::LoopThreadPtr _thread;
    mutable autil::ThreadMutex _mutex;
    SearchServiceManagerPtr _searchServiceManager;
    WorkerMetricReporterPtr _metricReporter;
    int64_t _processInterval;
    uint64_t _queueMaxSize;
    uint64_t _dropRequestCount;
    uint64_t _totalRequestCount;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(CallDelegationThread);
} // namespace multi_call

#endif // ISEARCH_CALLDELEGATIONTHREAD_H
