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

#include <stddef.h>

#include "autil/Log.h"
#include "future_lite/CoroInterface.h"
#include "future_lite/Unit.h"
#include "indexlib/file_system/file/CompressFileReader.h"

namespace autil { namespace mem_pool {
class Pool;
}} // namespace autil::mem_pool
namespace indexlib { namespace file_system {
struct ReadOption;
}} // namespace indexlib::file_system

namespace indexlib { namespace file_system {

class NormalCompressFileReader : public CompressFileReader
{
public:
    NormalCompressFileReader(autil::mem_pool::Pool* pool = NULL) noexcept : CompressFileReader(pool) {}

    ~NormalCompressFileReader() noexcept {}

public:
    NormalCompressFileReader* CreateSessionReader(autil::mem_pool::Pool* pool) noexcept override;
    std::string GetLoadTypeString() const noexcept override { return std::string("normal_block"); }

private:
    void LoadBuffer(size_t offset, ReadOption option) noexcept(false) override;
    void LoadBufferFromMemory(size_t offset, uint8_t* buffer, uint32_t bufLen,
                              bool enableTrace) noexcept(false) override;

    FL_LAZY(ErrorCode) LoadBufferAsyncCoro(size_t offset, ReadOption option) noexcept override;

    future_lite::coro::Lazy<std::vector<ErrorCode>>
    BatchLoadBuffer(const std::vector<std::pair<size_t, util::BufferCompressor*>>& blockInfo,
                    ReadOption option) noexcept override;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<NormalCompressFileReader> NormalCompressFileReaderPtr;
}} // namespace indexlib::file_system
