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

#include "autil/CommonMacros.h"
#include "autil/MultiValueType.h"
#include "autil/PackDataFormatter.h"

namespace autil {

class PackDataReader {
public:
    template <typename T>
    static T read(const char *base, uint64_t offset) {
        PackOffset pOffset;
        pOffset.fromUInt64(offset);
        return read<T>(base, pOffset);
    }

    template <typename T>
    static autil::MultiValueType<T> readMultiValue(const char *base, uint64_t offset) {
        PackOffset pOffset;
        pOffset.fromUInt64(offset);
        return readMultiValue<T>(base, pOffset);
    }

    template <typename T>
    static autil::MultiValueType<T> readFixedMultiValue(
            const char *base, uint64_t offset, uint32_t fixedCount)
    {
        PackOffset pOffset;
        pOffset.fromUInt64(offset);
        return readFixedMultiValue<T>(base, pOffset, fixedCount);
    }
    
    template <typename T>
    static T read(const char *base, const PackOffset& pOffset) {
        if (unlikely(base == nullptr)) {
            return T();
        }
        assert(!pOffset.isImpactFormat());
        return *(reinterpret_cast<const T *>(base + pOffset.getOffset()));
    }
    
    template <typename T>
    static autil::MultiValueType<T> readMultiValue(const char *base, const PackOffset& pOffset) {
        if (unlikely(base == nullptr)) {
            return autil::MultiValueType<T>();
        }
        size_t dataOffset = PackDataFormatter::getVarLenDataCursor(pOffset, base);
        if (pOffset.needVarLenHeader()) {
            return autil::MultiValueType<T>(base + dataOffset);
        }
        assert(pOffset.isImpactFormat());
        size_t varDataLen = autil::PackDataFormatter::getImpactVarLenDataLen(pOffset, base);
        uint32_t varCount = varDataLen / sizeof(T);
        assert(varDataLen % sizeof(T) == 0);
        return autil::MultiValueType<T>(base + dataOffset, varCount);
    }

    template <typename T>
    static autil::MultiValueType<T> readFixedMultiValue(
            const char *base, const PackOffset& pOffset, uint32_t fixedCount)
    {
        if (unlikely(base == nullptr)) {
            return autil::MultiValueType<T>();
        }
        assert(!pOffset.isImpactFormat());
        return autil::MultiValueType<T>(base + pOffset.getOffset(), fixedCount);
    }
};

}
