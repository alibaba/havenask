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
#include <string>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/SynchronizedQueue.h"
#include "autil/Thread.h"
#include "indexlib/file_system/flush/DumpScheduler.h"
#include "indexlib/file_system/flush/Dumpable.h"

namespace indexlib { namespace file_system {

class AsyncDumpScheduler : public DumpScheduler
{
public:
    AsyncDumpScheduler() noexcept;
    ~AsyncDumpScheduler() noexcept;

public:
    bool Init() noexcept override;
    ErrorCode Push(const DumpablePtr& dumpTask) noexcept override;
    void WaitFinished() noexcept override;
    bool HasDumpTask() noexcept override;
    ErrorCode WaitDumpQueueEmpty() noexcept override;
    void SetDumpQueueCapacity(uint32_t capacity) noexcept;

private:
    void Dump(const DumpablePtr& dumpTask) noexcept;
    void DumpThread() noexcept;

private:
    std::atomic_bool _running;
    std::exception_ptr _exception;
    autil::ThreadPtr _dumpThread;
    autil::SynchronizedQueue<DumpablePtr> _dumpQueue;
    autil::ThreadCond _queueEmptyCond;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<AsyncDumpScheduler> AsyncDumpSchedulerPtr;
}} // namespace indexlib::file_system
