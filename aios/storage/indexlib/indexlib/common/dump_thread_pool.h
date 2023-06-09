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
#ifndef __INDEXLIB_DUMP_THREAD_POOL_H
#define __INDEXLIB_DUMP_THREAD_POOL_H

#include <memory>

#include "autil/CircularQueue.h"
#include "autil/Lock.h"
#include "autil/Thread.h"
#include "indexlib/common/dump_item.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/SimplePool.h"

namespace indexlib { namespace common {

class DumpThreadPool
{
    // TODO : change code style
public:
    enum { DEFAULT_THREADNUM = 4, DEFAULT_QUEUESIZE = 32 };

    enum STOP_TYPE {
        STOP_THREAD_ONLY = 0,
        STOP_AND_CLEAR_QUEUE,
        STOP_AFTER_QUEUE_EMPTY,
        STOP_AND_CLEAR_QUEUE_IGNORE_EXCEPTION
    };

    enum ERROR_TYPE {
        ERROR_NONE = 0,
        ERROR_POOL_HAS_STOP,
        ERROR_POOL_ITEM_IS_NULL,
        ERROR_POOL_QUEUE_FULL,
    };

public:
    DumpThreadPool(const size_t threadNum = DEFAULT_THREADNUM, const size_t queueSize = DEFAULT_QUEUESIZE);
    ~DumpThreadPool();

private:
    DumpThreadPool(const DumpThreadPool&);
    DumpThreadPool& operator=(const DumpThreadPool&);

public:
    ERROR_TYPE pushWorkItem(DumpItem* item, bool isBlocked = true);
    size_t getItemCount() const;
    size_t getThreadNum() const { return _threadNum; }
    size_t getQueueSize() const { return _queueSize; }
    bool start(const std::string& name);
    void stop(STOP_TYPE stopType = STOP_AFTER_QUEUE_EMPTY);
    void clearQueue();
    size_t getPeakOfMemoryUse() const { return _peakMemoryUse; }

private:
    void push(DumpItem* item);
    void pushFront(DumpItem* item);
    bool tryPopItem(DumpItem*&);
    bool createThreads(const std::string& name);
    void workerLoop();
    void waitQueueEmpty();
    void stopThread();
    void consumItem(DumpItem* item, util::SimplePool* pool);
    void checkException();

private:
    size_t _threadNum;
    size_t _queueSize;
    autil::CircularQueue<DumpItem*> _queue;
    std::vector<autil::ThreadPtr> _threads;
    mutable autil::ProducerConsumerCond _cond;
    volatile bool _push;
    volatile bool _run;
    volatile bool _hasException;
    util::ExceptionBase _exception;
    size_t _peakMemoryUse;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DumpThreadPool);
}} // namespace indexlib::common

#endif //__INDEXLIB_DUMP_THREAD_POOL_H
