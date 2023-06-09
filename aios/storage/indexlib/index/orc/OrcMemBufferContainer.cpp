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
#include "indexlib/index/orc/OrcMemBufferContainer.h"

#include <chrono>
#include <thread>

#include "indexlib/index/orc/OrcConfig.h"
#include "indexlib/index/orc/OrcMemBuffer.h"
#include "orc/Writer.hh"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, OrcMemBufferContainer);

OrcMemBufferContainer::OrcMemBufferContainer(uint64_t size, uint64_t numRowsPerBatch)
    : _stop(true)
    , _size(size)
    , _numRowsPerBatch(numRowsPerBatch)
    , _thread(nullptr)
{
}

OrcMemBufferContainer::~OrcMemBufferContainer() { Stop(); }

Status OrcMemBufferContainer::Init(const indexlibv2::config::OrcConfig& config, orc::Writer* writer)
{
    _writer = writer;
    AUTIL_LOG(INFO, "init orc container, column num [%lu], size: [%lu]", config.GetFieldConfigs().size(), _size);
    for (size_t i = 0; i < _size; i++) {
        auto memBuffer = std::make_shared<OrcMemBuffer>(_numRowsPerBatch);
        auto status = memBuffer->Init(config, writer);
        if (!status.IsOK()) {
            return status;
        }
        _buildBufferQueue.Push(memBuffer);
    }
    _thread = autil::Thread::createThread(std::bind(&OrcMemBufferContainer::Seal, this), "OrcSeal");
    if (!_thread) {
        AUTIL_LOG(ERROR, "create orc seal thread failed");
        return Status::Corruption();
    }
    _stop = false;
    return Status::OK();
}

void OrcMemBufferContainer::Seal()
{
    while (true) {
        std::shared_ptr<OrcMemBuffer> buffer = _sealBufferQueue.Pop();
        if (buffer == nullptr) {
            break;
        }
        buffer->Seal();
        _writer->add(*(buffer->GetColumnVectorBatch()));
        buffer->Reset();
        _buildBufferQueue.Push(buffer);
    }
}

void OrcMemBufferContainer::Stop()
{
    if (_stop) {
        return;
    }
    _stop = true;
    _sealBufferQueue.Push(nullptr);
    if (_thread != nullptr) {
        _thread->join();
        _thread.reset();
    }
}

} // namespace indexlibv2::index
