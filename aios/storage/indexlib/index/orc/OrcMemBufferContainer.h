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

#include <atomic>
#include <queue>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "autil/Thread.h"
#include "indexlib/base/Status.h"

namespace orc {
class Writer;
}

namespace indexlibv2::config {
class OrcConfig;
}

namespace indexlibv2::index {
class OrcMemBuffer;

template <class T>
class SyncQueue
{
public:
    SyncQueue() = default;
    ~SyncQueue() = default;

    void Push(const std::shared_ptr<OrcMemBuffer>& buffer)
    {
        autil::ScopedLock lock(_cond);
        _queue.push(buffer);
        _size++;
        _cond.signal();
    }

    std::shared_ptr<OrcMemBuffer> Pop()
    {
        autil::ScopedLock lock(_cond);
        if (_queue.empty()) {
            _cond.wait();
        }
        assert(!_queue.empty());
        auto ret = _queue.front();
        _queue.pop();
        _size--;
        return ret;
    }

    size_t Size() { return _size.load(); }

private:
    autil::ThreadCond _cond;
    std::queue<T> _queue;
    std::atomic<size_t> _size;
};

class OrcMemBufferContainer : private autil::NoCopyable
{
public:
    OrcMemBufferContainer(uint64_t size, uint64_t numRowsPerBatch);
    ~OrcMemBufferContainer();

    Status Init(const indexlibv2::config::OrcConfig& config, orc::Writer* writer);

    void Push(const std::shared_ptr<OrcMemBuffer>& orcMemBuffer) { _sealBufferQueue.Push(orcMemBuffer); }
    std::shared_ptr<OrcMemBuffer> Pop() { return _buildBufferQueue.Pop(); }
    void Seal();
    void Stop();

private:
    bool _stop;
    uint64_t _size;
    uint64_t _numRowsPerBatch;
    autil::ThreadPtr _thread;
    orc::Writer* _writer;
    SyncQueue<std::shared_ptr<OrcMemBuffer>> _buildBufferQueue;
    SyncQueue<std::shared_ptr<OrcMemBuffer>> _sealBufferQueue;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
