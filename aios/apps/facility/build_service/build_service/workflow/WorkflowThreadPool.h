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
#ifndef ISEARCH_BS_WORKFLOWTHREADPOOL_H
#define ISEARCH_BS_WORKFLOWTHREADPOOL_H

#include <memory>

#include "autil/CircularQueue.h"
#include "autil/Lock.h"
#include "autil/Thread.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace workflow {
class WorkflowItemBase;

class WorkflowThreadPool
{
public:
    enum { DEFAULT_THREADNUM = 4, DEFAULT_QUEUESIZE = 1024 };

public:
    WorkflowThreadPool(const size_t threadNum = DEFAULT_THREADNUM, const size_t queueSize = DEFAULT_QUEUESIZE);
    ~WorkflowThreadPool();

private:
    WorkflowThreadPool(const WorkflowThreadPool&);
    WorkflowThreadPool& operator=(const WorkflowThreadPool&);

public:
    bool pushWorkItem(WorkflowItemBase* item);
    size_t getItemCount() const;
    size_t getThreadNum() const { return _threadNum; }
    size_t getQueueSize() const { return _queueSize; }
    size_t getMaxItemCount() const { return _maxItemCount; }
    bool start();
    bool start(const std::string& name);

private:
    bool createThreads(const std::string& name);
    void workerLoop();
    void stopThread();
    void clearQueue();
    WorkflowItemBase* popOneItem();

private:
    size_t _threadNum;
    size_t _queueSize;
    size_t _maxItemCount;
    size_t _waitInterval;
    autil::CircularQueue<WorkflowItemBase*> _queue;
    std::vector<autil::ThreadPtr> _threads;
    mutable autil::ThreadMutex _mutex;
    volatile bool _run;

private:
    static const int64_t DEFAULT_WAIT_INTERVAL_MS;

private:
    BS_LOG_DECLARE();
};

typedef std::shared_ptr<WorkflowThreadPool> WorkflowThreadPoolPtr;

}} // namespace build_service::workflow

#endif //__INDEXLIB_DUMP_THREAD_POOL_H
