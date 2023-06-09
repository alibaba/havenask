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
#include "autil/OutputOrderedThreadPool.h"

#include <cstddef>

#include "autil/Log.h"

using namespace std;

namespace autil {
AUTIL_DECLARE_AND_SETUP_LOGGER(autil, OutputOrderedThreadPool);

OutputOrderedThreadPool::OutputOrderedThreadPool(
        uint32_t threadNum, uint32_t queueSize)
    : _queue(queueSize)
    , _threadPool(threadNum, queueSize)
{
    _waitItem = NULL;
    _running = false;
}

OutputOrderedThreadPool::~OutputOrderedThreadPool() { 
    _running = false;
    _threadPool.stop();
}

bool OutputOrderedThreadPool::start(const string &name) {
    AUTIL_LOG(INFO, "OutputOrderedThreadPool begin start");
    if (_queue.capacity() < 1) {
        return false;
    }

    _running = _threadPool.start(name);
    return _running;
}

bool OutputOrderedThreadPool::pushWorkItem(
        WorkItem *item) 
{
    if (!item || !_running) return false;
    OrderWorkItem *oItem = NULL;
    {
        ScopedLock lock(_queueCond);
        while (_queue.size() == _queue.capacity()) {
            _queueCond.producerWait();
            if (!_running) return false;
        }
        if (!_running) {
            return false;
        }
        _queue.push_back(OrderWorkItem(item, this));
        oItem = &_queue.back();
        _waitItemCount.inc();
    }
    //assert _threadPool is running    
    _threadPool.pushWorkItem(oItem, true);
    return true;
}

WorkItem* OutputOrderedThreadPool::popWorkItem() {
    ScopedLock lock(_queueCond);
    while (_queue.empty() || !_queue.front()._processed) {
        if (_queue.empty()) {
            if (!_running) {return NULL;}
            _waitItem = NULL;
        } else {
            _waitItem = &_queue.front();
        }
        _queueCond.consumerWait();
    }
    
    WorkItem *item = _queue.front()._item;
    _queue.pop_front();
    _outputItemCount.dec();
    _queueCond.signalProducer();
    return item;
}

bool OutputOrderedThreadPool::waitStop(ThreadPool::STOP_TYPE stopType)
{
    _running = false;
    _threadPool.stop(stopType);
    ScopedLock lock(_queueCond);
    if (stopType == ThreadPool::STOP_AND_CLEAR_QUEUE_IGNORE_EXCEPTION || stopType == ThreadPool::STOP_AND_CLEAR_QUEUE) {
        while (!_queue.empty()) {
            auto item = _queue.front()._item;
            _queue.pop_front();
            item->destroy();
        }
    }
    _queueCond.broadcastProducer();
    _queueCond.broadcastConsumer();
    return true;
}

}
