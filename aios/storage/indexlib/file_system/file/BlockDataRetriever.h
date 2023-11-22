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

#include <memory>

#include "future_lite/coro/Lazy.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/file/DecompressMetricsReporter.h"

namespace indexlib { namespace file_system {

class BlockDataRetriever
{
public:
    BlockDataRetriever(size_t blockSize, size_t totalLength) noexcept : _blockSize(blockSize), _totalLength(totalLength)
    {
    }

    virtual ~BlockDataRetriever() noexcept {}

    BlockDataRetriever(const BlockDataRetriever&) = delete;
    BlockDataRetriever& operator=(const BlockDataRetriever&) = delete;
    BlockDataRetriever(BlockDataRetriever&&) = delete;
    BlockDataRetriever& operator=(BlockDataRetriever&&) = delete;

public:
    virtual FSResult<uint8_t*> RetrieveBlockData(size_t fileOffset, size_t& blockDataBeginOffset,
                                                 size_t& blockDataLength) noexcept = 0;

    virtual future_lite::coro::Lazy<ErrorCode> Prefetch(size_t fileOffset, size_t length) noexcept = 0;

    virtual void Reset() noexcept = 0;

    void SetMetricsReporter(const DecompressMetricsReporter& reporter) noexcept { _reporter = reporter; }

protected:
    bool GetBlockMeta(size_t fileOffset, size_t& blockDataBeginOffset, size_t& blockDataLen) const noexcept
    {
        if (unlikely(fileOffset >= _totalLength)) {
            return false;
        }
        blockDataBeginOffset = fileOffset - (fileOffset % _blockSize);
        blockDataLen = std::min(_totalLength - blockDataBeginOffset, _blockSize);
        return true;
    }

protected:
    size_t _blockSize;
    size_t _totalLength;
    DecompressMetricsReporter _reporter;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<BlockDataRetriever> BlockDataRetrieverPtr;
}} // namespace indexlib::file_system
