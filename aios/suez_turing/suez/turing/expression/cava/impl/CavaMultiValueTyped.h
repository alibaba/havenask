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
#include <stdint.h>

#include "autil/MultiValueFormatter.h"
#include "autil/MultiValueType.h"
#include "cava/common/Common.h"
#include "cava/runtime/CavaArrayType.h"

class CavaCtx;
namespace cava {
namespace lang {
class CString;
} // namespace lang
} // namespace cava

namespace ha3 {
class MDouble;
class MFloat;
class MInt16;
class MInt32;
class MInt64;
class MInt8;
class MUInt16;
class MUInt32;
class MUInt64;
class MUInt8;

#define MULTI_VALUE(name, type)                                                                                        \
    class M##name {                                                                                                    \
    public:                                                                                                            \
        M##name(autil::MultiValueType<type> *val) { reset(val); }                                                      \
        M##name() : _bufferOffset(0), _size(0), _data(NULL) {}                                                         \
        ~M##name() {}                                                                                                  \
                                                                                                                       \
    public:                                                                                                            \
        void reset(type *data, uint32_t size, uint32_t countLen) {                                                     \
            if (data) {                                                                                                \
                _size = size;                                                                                          \
                _data = data;                                                                                          \
                _bufferOffset = countLen;                                                                              \
            } else {                                                                                                   \
                _bufferOffset = 0;                                                                                     \
                _size = 0;                                                                                             \
                _data = NULL;                                                                                          \
            }                                                                                                          \
        }                                                                                                              \
        void reset(autil::MultiValueType<type> *val) {                                                                 \
            const char *data = val->getData();                                                                         \
            if (data) {                                                                                                \
                _size = val->getEncodedCountValue();                                                                   \
                _bufferOffset = autil::MultiValueFormatter::getEncodedCountLength(_size);                              \
                _data = (type *)data;                                                                                  \
            } else {                                                                                                   \
                _bufferOffset = 0;                                                                                     \
                _size = 0;                                                                                             \
                _data = NULL;                                                                                          \
            }                                                                                                          \
        }                                                                                                              \
        type getWithoutCheck(CavaCtx *ctx, uint32_t index);                                                            \
        void set(CavaCtx *ctx, uint32_t index, type value);                                                            \
        uint32_t size(CavaCtx *ctx);                                                                                   \
        static M##name *create(CavaCtx *ctx, uint32_t len);                                                            \
                                                                                                                       \
    public:                                                                                                            \
        char *getMultiValueBuffer();                                                                                   \
        void setConst() { _bufferOffset |= 0x80000000; }                                                               \
        bool isConst() { return _bufferOffset & 0x80000000; }                                                          \
                                                                                                                       \
    private:                                                                                                           \
        bool init(CavaCtx *ctx, uint32_t len);                                                                         \
                                                                                                                       \
    private:                                                                                                           \
        size_t _bufferOffset;                                                                                          \
        uint32_t _size;                                                                                                \
        type *_data;                                                                                                   \
    };

MULTI_VALUE(Int8, cava::byte)
MULTI_VALUE(Int16, short)
MULTI_VALUE(Int32, int)
MULTI_VALUE(Int64, long)
MULTI_VALUE(UInt8, ubyte)
MULTI_VALUE(UInt16, ushort)
MULTI_VALUE(UInt32, uint)
MULTI_VALUE(UInt64, ulong)
MULTI_VALUE(Float, float)
MULTI_VALUE(Double, double)

class MChar {
public:
    // deprecated to be delete
    cava::lang::CString *toString(CavaCtx *ctx);
    cava::lang::CString *toCString(CavaCtx *ctx);
    static MChar *create(CavaCtx *ctx, cava::lang::CString *str);
    int compare(CavaCtx *ctx, cava::lang::CString *str);
};

class MString {
public:
    MString() {}
    MString(autil::MultiString *val);
    MChar *getWithoutCheck(CavaCtx *ctx, uint index);
    uint size(CavaCtx *ctx);
    static MString *create(CavaCtx *ctx, CavaArrayType<cava::lang::CString *> *strArray);

public:
    void reset(autil::MultiString *val) {
        assert(val->isEmptyData() || val->hasEncodedCount());
        _baseAddress = (char *)val->getBaseAddress();
        if (_baseAddress) {
            size_t countLen = 0;
            _size = autil::MultiValueFormatter::decodeCount(_baseAddress, countLen);
            if (_size > 0) { // the address of offsetItemLen can be invalid if size is zero
                char *offsetLenAddr = _baseAddress + countLen;
                _offsetAddr = offsetLenAddr + sizeof(uint8_t);
                _offsetItemLen = *(const uint8_t *)offsetLenAddr;
            } else {
                _offsetAddr = nullptr;
                _offsetItemLen = 0;
            }
        } else {
            _offsetAddr = NULL;
            _size = 0;
            _offsetItemLen = 0;
        }
    }
    void *getBaseAddress() { return _baseAddress; }

private:
    char *_baseAddress = nullptr;
    char *_offsetAddr = nullptr;
    uint32_t _size = 0;
    uint8_t _offsetItemLen = 0;
};
} // namespace ha3
