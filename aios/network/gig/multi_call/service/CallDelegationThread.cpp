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
#include "aios/network/gig/multi_call/service/CallDelegationThread.h"
#include "autil/TimeUtility.h"
#include <unistd.h>

using namespace std;
using namespace autil;
namespace multi_call {
AUTIL_LOG_SETUP(multi_call, CallDelegationThread);

CallDelegationThread::CallDelegationThread(
    const SearchServiceManagerPtr &searchServiceManager,
    const WorkerMetricReporterPtr &metricReporter, int64_t processInterval, uint64_t queueSize)
    : _searchServiceManager(searchServiceManager),
      _metricReporter(metricReporter), _processInterval(processInterval) ,
      _queueMaxSize(queueSize), _dropRequestCount(0) , _totalRequestCount(0) {}

CallDelegationThread::~CallDelegationThread() { stop(); }

bool CallDelegationThread::start() {
    if (!_searchServiceManager) {
        AUTIL_LOG(ERROR, "search service manager is NULL");
        return false;
    }
    _thread = LoopThread::createLoopThread(
        std::bind(&CallDelegationThread::workLoop, this), _processInterval, "GigCallDeg");
    if (!_thread) {
        AUTIL_LOG(ERROR, "create delegation thread failed!");
        return false;
    }
    AUTIL_LOG(
        INFO,
        "GigCallDeg thread start with interval [%lu] queue max size [%lu] done",
        _processInterval, _queueMaxSize);
    return true;
}

void CallDelegationThread::stop() {
    if (_thread) {
        while (!isAllQueueEmpty()) {
            usleep(10 * 1000);
        }
        _thread->stop();
        _thread.reset();
    }
    mergeQueue();
    autil::ScopedLock lock(_mutex);
    for (const auto item : _queue) {
        delete item;
        AUTIL_LOG(INFO, "work item dropped");
    }
    _queue.clear();
}

void CallDelegationThread::workLoop() {
    int64_t currentTime = TimeUtility::currentTime();
    SearchServiceSnapshotPtr snapshot =
        _searchServiceManager->getSearchServiceSnapshot();
    for (CallDelegationQueue::iterator it = _queue.begin();
         it != _queue.end();) {
        CallDelegationWorkItem *workItem = *it;
        assert(workItem);
        if (workItem->process(currentTime, snapshot)) {
            delete workItem;
            it = _queue.erase(it);
        } else {
            ++it;
        }
    }
    mergeQueue();
    uint64_t queueSize = _queue.size();
    if (_metricReporter) {
        _metricReporter->reportQueueSize(queueSize);
    }
}

void CallDelegationThread::pushWorkItem(CallDelegationWorkItem *workItem)
{
    size_t pushQueueSize = 0;
    {
        ScopedLock lock(_mutex);
        pushQueueSize = _pushQueue.size();

        _totalRequestCount++;
        if (pushQueueSize >= _queueMaxSize) {
            _dropRequestCount++;
            delete workItem;
            AUTIL_INTERVAL_LOG(100000, WARN,
                       "push queue size [%lu], total request "
                       "count[%lu], drop request count[%lu]",
                       pushQueueSize, _totalRequestCount,
                       _dropRequestCount);
            return;
        } else {
            AUTIL_LOG(DEBUG, "add new CallDelegationWorkItem");
            _pushQueue.push_back(workItem);
        }
    }
}

void CallDelegationThread::mergeQueue() {
    autil::ScopedLock lock(_mutex);
    _queue.insert(_queue.end(), _pushQueue.begin(), _pushQueue.end());
    _pushQueue.clear();
}

bool CallDelegationThread::isAllQueueEmpty() const {
    autil::ScopedLock lock(_mutex);
    return _queue.empty() && _pushQueue.empty();
}

} // namespace multi_call
