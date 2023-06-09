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

#include <cassert>
#include <memory>

#include "autil/Log.h"

namespace indexlib { namespace file_system {

class CompressFileAddressMapperEncoder
{
public:
    CompressFileAddressMapperEncoder(size_t maxBatchNum = 16);
    ~CompressFileAddressMapperEncoder();

    CompressFileAddressMapperEncoder(const CompressFileAddressMapperEncoder&) = delete;
    CompressFileAddressMapperEncoder& operator=(const CompressFileAddressMapperEncoder&) = delete;
    CompressFileAddressMapperEncoder(CompressFileAddressMapperEncoder&&) = delete;
    CompressFileAddressMapperEncoder& operator=(CompressFileAddressMapperEncoder&&) = delete;

public:
    size_t CalculateEncodeSize(size_t num) noexcept;
    bool Encode(size_t* offsetVec, size_t num, char* outputBuffer, size_t outputBufferSize) noexcept;
    inline size_t GetOffset(char* encodeData, size_t idx) noexcept;

private:
    size_t GetCurrentBatchSize(size_t num) noexcept
    {
        assert(num <= _maxBatchNum);
        if (num == 0) {
            return 0;
        }
        return sizeof(size_t) + (num - 1) * sizeof(uint16_t);
    }

private:
    size_t _maxBatchNum;
    size_t _maxBatchSize;

private:
    AUTIL_LOG_DECLARE();
};

inline size_t CompressFileAddressMapperEncoder::GetOffset(char* encodeData, size_t idx) noexcept
{
    size_t blockIdx = idx / _maxBatchNum;
    size_t inBlockIdx = idx % _maxBatchNum;
    char* baseAddr = encodeData + (blockIdx * _maxBatchSize);
    size_t baseValue = *(size_t*)baseAddr;
    uint16_t* deltaAddr = (uint16_t*)(baseAddr + sizeof(size_t));
    return inBlockIdx == 0 ? baseValue : (baseValue + deltaAddr[inBlockIdx - 1]);
}

typedef std::shared_ptr<CompressFileAddressMapperEncoder> CompressFileAddressMapperEncoderPtr;

}} // namespace indexlib::file_system
