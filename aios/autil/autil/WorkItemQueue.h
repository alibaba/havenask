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

#include <deque>

#include "autil/WorkItem.h"
#include "autil/CircularQueue.h"

namespace autil {

class WorkItemQueue {
public:
    WorkItemQueue(){};
    virtual ~WorkItemQueue(){};
    virtual bool push(WorkItem *item) = 0;
    virtual WorkItem *pop() = 0;
    virtual size_t size() const = 0;
};

class WorkItemQueueFactory {
public:
    virtual ~WorkItemQueueFactory(){};
    virtual WorkItemQueue *create(const size_t threadNum, const size_t queueSize) = 0;
};
typedef std::shared_ptr<WorkItemQueueFactory> WorkItemQueueFactoryPtr;

class CircularPoolQueue : public WorkItemQueue {
public:
    CircularPoolQueue(const size_t queueSize) : _queue(queueSize){};
    virtual bool push(WorkItem *item) override {
        if (_queue.size() >= _queue.capacity()) {
            return false;
        }
        _queue.push_back(item);
        return true;
    }
    virtual WorkItem *pop() override {
        if (_queue.empty()) {
            return nullptr;
        }
        WorkItem *item = _queue.front();
        _queue.pop_front();
        return item;
    }
    virtual size_t size() const override{ return _queue.size(); }

private:
    CircularQueue<WorkItem *> _queue;
};

class ThreadPoolQueue : public WorkItemQueue {
public:
    ThreadPoolQueue(){};
    virtual bool push(WorkItem *item) override {
        _queue.push_back(item);
        return true;
    }
    virtual WorkItem *pop() override {
        if (_queue.empty()) {
            return nullptr;
        }
        WorkItem *item = _queue.front();
        _queue.pop_front();
        return item;
    }
    virtual size_t size() const override { return _queue.size(); }

private:
    std::deque<WorkItem *> _queue;
};

class ThreadPoolQueueFactory : public WorkItemQueueFactory {
public:
    WorkItemQueue *create(const size_t threadNum, const size_t queueSize) override { return new ThreadPoolQueue(); }
};

} // namespace autil
