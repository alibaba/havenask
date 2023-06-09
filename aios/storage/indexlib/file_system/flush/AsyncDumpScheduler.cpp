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
#include "indexlib/file_system/flush/AsyncDumpScheduler.h"

#include <algorithm>
#include <functional>
#include <iosfwd>
#include <memory>
#include <stdint.h>
#include <string>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/EnvUtil.h"
#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/SynchronizedQueue.h"
#include "autil/Thread.h"
#include "autil/legacy/exception.h"
#include "beeper/beeper.h"
#include "beeper/common/common_type.h"
#include "indexlib/file_system/flush/Dumpable.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, AsyncDumpScheduler);

namespace {
static const uint32_t DUMP_QUEUE_SIZE = []() {
    uint32_t ret = autil::EnvUtil::getEnv("indexlib_file_system_aynsc_dump_queue", 32);
    return ret;
}();
}

AsyncDumpScheduler::AsyncDumpScheduler() noexcept {}

AsyncDumpScheduler::~AsyncDumpScheduler() noexcept { WaitFinished(); }

bool AsyncDumpScheduler::Init() noexcept
{
    SetDumpQueueCapacity(DUMP_QUEUE_SIZE);
    _running = true;
    _dumpThread = autil::Thread::createThread(std::bind(&AsyncDumpScheduler::DumpThread, this), "indexAsyncDump");
    _running = true;
    if (!_dumpThread) {
        AUTIL_LOG(ERROR, "create index async dump thread failed");
        _running = false;
        return false;
    }
    return true;
}

ErrorCode AsyncDumpScheduler::Push(const DumpablePtr& dumpTask) noexcept
{
    _dumpQueue.push(dumpTask);
    return FSEC_OK;
}

void AsyncDumpScheduler::WaitFinished() noexcept
{
    _running = false;
    _dumpQueue.wakeup();
    _dumpThread->join();
}

void AsyncDumpScheduler::SetDumpQueueCapacity(uint32_t capacity) noexcept { _dumpQueue.setCapacity(capacity); }

void AsyncDumpScheduler::Dump(const DumpablePtr& dumpTask) noexcept
{
    if (_exception) {
        AUTIL_LOG(ERROR, "Catch a exception before, drop this dump task");
        return;
    }
    try {
        dumpTask->Dump();
    } catch (const autil::legacy::ExceptionBase& e) {
        AUTIL_LOG(ERROR, "Catch exception: %s", e.what());
        _exception = std::current_exception();
        beeper::EventTags tags;
        BEEPER_FORMAT_REPORT("bs_worker_error", tags, "AsyncDump catch exception: %s", e.what());
    } catch (...) {
        AUTIL_LOG(ERROR, "Catch unknown exception");
        _exception = std::current_exception();
        BEEPER_REPORT("bs_worker_error", "AsyncDump catch unknown exception");
    }
}

void AsyncDumpScheduler::DumpThread() noexcept
{
    while (_running || !_dumpQueue.isEmpty()) {
        while (_dumpQueue.isEmpty() && _running) {
            _dumpQueue.waitNotEmpty();
        }
        while (!_dumpQueue.isEmpty()) {
            DumpablePtr dumpTask = _dumpQueue.getFront();
            Dump(dumpTask);

            _queueEmptyCond.lock();
            _dumpQueue.popFront();
            _queueEmptyCond.signal();
            _queueEmptyCond.unlock();
        }
    }
}

bool AsyncDumpScheduler::HasDumpTask() noexcept { return !_dumpQueue.isEmpty(); }

ErrorCode AsyncDumpScheduler::WaitDumpQueueEmpty() noexcept
{
    _queueEmptyCond.lock();
    while (!_dumpQueue.isEmpty()) {
        _queueEmptyCond.wait(100000);
    }
    _queueEmptyCond.unlock();
    if (_exception) {
        RETURN_IF_FS_EXCEPTION(std::rethrow_exception(_exception), "Dump failed");
    }
    return FSEC_OK;
}
}} // namespace indexlib::file_system
