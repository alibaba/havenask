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

#include <assert.h>
#include <endian.h>
#include <stddef.h>
#include <stdint.h>

namespace autil {

struct PackOffset {
public:
    PackOffset() : PackOffset(0, 0, 0, true, false)
    {}

#if __BYTE_ORDER == __BIG_ENDIAN
    PackOffset(size_t _offset, size_t _varIdx, size_t _varNum, bool needVarHeader, bool impact)
        : isImpact(impact ? 1 : 0)
        , varHeader(needVarHeader ? 0 : 1)
        , varNum(_varNum)
        , varIndex(_varIdx)        
        , offset(_offset)
    {}
#else
    PackOffset(size_t _offset, size_t _varIdx, size_t _varNum, bool needVarHeader, bool impact)
        : offset(_offset)
        , varIndex(_varIdx)
        , varNum(_varNum)
        , varHeader(needVarHeader ? 0 : 1)
        , isImpact(impact ? 1 : 0)
    {}
#endif
    
    bool needVarLenHeader() const { return varHeader == 0; }
    bool isImpactFormat() const { return isImpact == 1; }
    size_t getOffset() const { return offset; }
    size_t getVarLenFieldIdx() const { return varIndex; }
    size_t getVarLenFieldNum() const { return varNum; }

    uint64_t toUInt64() const {
        return *reinterpret_cast<uint64_t*>(const_cast<PackOffset*>(this));
    }

    void fromUInt64(uint64_t value) {
        PackOffset* ptr = reinterpret_cast<PackOffset*>(&value);
        *this = *ptr;
    }
    
public:
    static PackOffset normalOffset(size_t offset, bool needHeader = true) {
        return PackOffset(offset, 0, 0, needHeader, false);
    }

    static PackOffset impactOffset(size_t fixOffset, size_t idx, size_t varNum, bool needHeader) {
        assert(idx < varNum);
        bool isLastVarField = ((idx + 1) == varNum);
        assert(!isLastVarField || needHeader);
        needHeader = isLastVarField || needHeader; // last var field must need Header
        return PackOffset(fixOffset, idx, varNum, needHeader, true);
    }

private:
#if __BYTE_ORDER == __BIG_ENDIAN
    size_t isImpact  : 1;  // 0: normal, 1: impact
    size_t varHeader : 1;  // 0: hasCountHeader for var field, 1: no count
    size_t varNum    : 16; // total number for var field
    size_t varIndex  : 16; // index for var field, begin from 0
    size_t offset    : 30;
#else
    size_t offset    : 30;
    size_t varIndex  : 16; // index for var field, begin from 0
    size_t varNum    : 16; // total number for var field
    size_t varHeader : 1;  // 0: hasCountHeader for var field, 1: no count
    size_t isImpact  : 1;  // 0: normal, 1: impact
#endif
};

////////////////////////////////////////////////////////////////////////////////////////////
class PackDataFormatter
{
public:
    static void setVarLenOffset(const PackOffset& pOffset, char* baseAddr, size_t dataCursor);
    static inline size_t getVarLenDataCursor(const PackOffset& pOffset, const char* baseAddr);
    static inline size_t getImpactVarLenDataLen(const PackOffset& pOffset, const char* baseAddr);
};

inline size_t PackDataFormatter::getVarLenDataCursor(const PackOffset& pOffset, const char* baseAddr)
{
    size_t offset = pOffset.getOffset();
    size_t varIndex = pOffset.getVarLenFieldIdx();
    size_t varNum = pOffset.getVarLenFieldNum();
    if (pOffset.isImpactFormat()) {
        if (varNum == 1) {
            return offset;
        }
        size_t offsetLen = (size_t)baseAddr[offset];
        assert(offsetLen == 1 || offsetLen == 2 || offsetLen == 4);
        size_t offsetEndCursor = offset + sizeof(char) + (varNum - 1) * offsetLen;
        if (varIndex == 0) {
            return offsetEndCursor;
        }
        const char* dataBuf = baseAddr + offset + sizeof(char);
        switch (offsetLen) {
        case 1: {
            uint8_t* addr = (uint8_t*)dataBuf;
            return offsetEndCursor + addr[varIndex - 1];
        }
        case 2: {
            uint16_t* addr = (uint16_t*)dataBuf;
            return offsetEndCursor + addr[varIndex - 1];
        }
        case 4: {
            uint32_t* addr = (uint32_t*)dataBuf;
            return offsetEndCursor + addr[varIndex - 1];
        }
        default:
            assert(false);
        }
        return 0;
    } else {
        int64_t cursor = *(reinterpret_cast<const int64_t*>(baseAddr + offset));
        return offset + (size_t)cursor;
    }
}

inline size_t PackDataFormatter::getImpactVarLenDataLen(const PackOffset& pOffset, const char* baseAddr)
{
    size_t offset = pOffset.getOffset();
    size_t varIndex = pOffset.getVarLenFieldIdx();
    size_t varNum = pOffset.getVarLenFieldNum();
    assert(pOffset.isImpactFormat());
    assert(varNum > 1 && (varIndex + 1) != varNum);
    (void)varNum;
    size_t offsetLen = (size_t)baseAddr[offset];
    assert(offsetLen == 1 || offsetLen == 2 || offsetLen == 4);
    const char* dataBuf = baseAddr + offset + sizeof(char);
    switch (offsetLen) {
    case 1: {
        uint8_t* addr = (uint8_t*)dataBuf;
        return addr[varIndex] - (varIndex == 0 ? 0 : addr[varIndex - 1]);
    }
    case 2: {
        uint16_t* addr = (uint16_t*)dataBuf;
        return addr[varIndex] - (varIndex == 0 ? 0 : addr[varIndex - 1]);
    }
    case 4: {
        uint32_t* addr = (uint32_t*)dataBuf;
        return addr[varIndex] - (varIndex == 0 ? 0 : addr[varIndex - 1]);
    }
    default:
        assert(false);
    }
    return 0;
}

}
