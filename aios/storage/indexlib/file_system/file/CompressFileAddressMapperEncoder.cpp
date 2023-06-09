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
#include "indexlib/file_system/file/CompressFileAddressMapperEncoder.h"

#include <limits>

using namespace std;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, CompressFileAddressMapperEncoder);

CompressFileAddressMapperEncoder::CompressFileAddressMapperEncoder(size_t maxBatchNum)
    : _maxBatchNum(maxBatchNum)
    , _maxBatchSize(GetCurrentBatchSize(maxBatchNum))
{
    if (_maxBatchNum == 0 || _maxBatchNum > 128) {
        _maxBatchNum = 16;
        _maxBatchSize = GetCurrentBatchSize(_maxBatchNum);
    }
}

CompressFileAddressMapperEncoder::~CompressFileAddressMapperEncoder() {}

size_t CompressFileAddressMapperEncoder::CalculateEncodeSize(size_t num) noexcept
{
    if (num == 0) {
        return 0;
    }
    size_t wholeBatchNum = num / _maxBatchNum;
    size_t remainNum = num % _maxBatchNum;
    return wholeBatchNum * _maxBatchSize + GetCurrentBatchSize(remainNum);
}

bool CompressFileAddressMapperEncoder::Encode(size_t* offsetVec, size_t num, char* outputBuffer,
                                              size_t outputBufferSize) noexcept
{
    if (outputBufferSize < CalculateEncodeSize(num)) {
        return false;
    }
    size_t base = 0;
    char* cursor = outputBuffer;
    for (size_t i = 0; i < num; i++) {
        if (i % _maxBatchNum == 0) {
            base = offsetVec[i];
            *(size_t*)cursor = base;
            cursor += sizeof(size_t);
        } else {
            if (offsetVec[i] < base) {
                return false;
            }
            size_t delta = offsetVec[i] - base;
            if (delta > numeric_limits<uint16_t>::max()) {
                return false;
            }
            *(uint16_t*)cursor = (uint16_t)delta;
            cursor += sizeof(uint16_t);
        }
    }
    return true;
}

}} // namespace indexlib::file_system
