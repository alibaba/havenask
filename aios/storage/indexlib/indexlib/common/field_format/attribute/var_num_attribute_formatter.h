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

#include "autil/MultiValueFormatter.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace common {

class VarNumAttributeFormatter
{
public:
    static const size_t VAR_NUM_ATTRIBUTE_SLICE_LEN = 64 * 1024 * 1024;
    static const size_t VAR_NUM_NULL_FIELD_VALUE_COUNT = autil::MultiValueFormatter::VAR_NUM_NULL_FIELD_VALUE_COUNT;
    static const size_t VAR_NUM_ATTRIBUTE_MAX_COUNT = VAR_NUM_NULL_FIELD_VALUE_COUNT - 1;

public:
    VarNumAttributeFormatter() {}
    ~VarNumAttributeFormatter() {}

public:
    static inline size_t GetEncodedCountLength(uint32_t count)
    {
        return autil::MultiValueFormatter::getEncodedCountLength(count);
    }

    static inline size_t GetEncodedCountFromFirstByte(uint8_t firstByte)
    {
        return autil::MultiValueFormatter::getEncodedCountFromFirstByte(firstByte);
    }

    static inline size_t EncodeCount(uint32_t count, char* buffer, size_t bufferSize)
    {
        return autil::MultiValueFormatter::encodeCount(count, buffer, bufferSize);
    }

    static inline uint32_t DecodeCount(const char* buffer, size_t& countLen, bool& isNull)
    {
        uint32_t count = autil::MultiValueFormatter::decodeCount(buffer, countLen);
        isNull = (count == VAR_NUM_NULL_FIELD_VALUE_COUNT);
        if (isNull) {
            count = 0;
        }
        return count;
    }

    static inline size_t GetOffsetItemLength(uint32_t offset)
    {
        return autil::MultiValueFormatter::getOffsetItemLength(offset);
    }

    static inline uint32_t GetOffset(const char* offsetBaseAddr, size_t offsetItemLen, size_t idx)
    {
        return autil::MultiValueFormatter::getOffset(offsetBaseAddr, offsetItemLen, idx);
    }

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(VarNumAttributeFormatter);
}} // namespace indexlib::common
