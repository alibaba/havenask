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
#include "suez/turing/expression/cava/impl/CavaMultiValueTyped.h"

#include <algorithm>
#include <assert.h>
#include <string.h>

#include "autil/CommonMacros.h"
#include "autil/ConstString.h"
#include "autil/LongHashValue.h"
#include "autil/MultiValueFormatter.h"
#include "autil/MultiValueWriter.h"
#include "cava/common/ErrorDef.h"
#include "cava/runtime/CString.h"
#include "cava/runtime/CavaArrayType.h"
#include "cava/runtime/CavaCtx.h"
#include "suez/turing/expression/cava/common/CavaFieldTypeHelper.h"
#include "suez/turing/expression/cava/common/SuezCavaAllocator.h"

using namespace suez::turing;
using namespace cava;
using namespace cava::lang;
using namespace autil;

namespace ha3 {

#define MULTI_VALUE_CREATE(name, type)                                                                                 \
    void ha3::M##name::set(CavaCtx *ctx, uint32_t index, type value) {                                                 \
        if (unlikely(index >= _size || index < 0 || isConst())) {                                                      \
            ctx->exception = ExceptionCode::EXC_INVALID_ARGUMENT;                                                      \
            return;                                                                                                    \
        }                                                                                                              \
        _data[index] = value;                                                                                          \
    }                                                                                                                  \
    uint32_t ha3::M##name::size(CavaCtx *ctx) { return _size; }                                                        \
    char *ha3::M##name::getMultiValueBuffer() { return (char *)_data - (_bufferOffset & (~0x80000000)); }              \
    type ha3::M##name::getWithoutCheck(CavaCtx *ctx, uint32_t index) { return _data[index]; }                          \
    bool ha3::M##name::init(CavaCtx *ctx, uint32_t len) {                                                              \
        _size = len;                                                                                                   \
        size_t bufLen = MultiValueFormatter::calculateBufferLen(_size, sizeof(type));                                  \
        auto buffer = (char *)suez::turing::SuezCavaAllocUtil::alloc(ctx, bufLen);                                     \
        if (unlikely(buffer == nullptr)) {                                                                             \
            return false;                                                                                              \
        }                                                                                                              \
        MultiValueWriter<type> multiValueWriter;                                                                       \
        if (!multiValueWriter.init(buffer, bufLen, _size)) {                                                           \
            ctx->setOtherException("multi-value buffer init failed");                                                  \
            _data = NULL;                                                                                              \
            _bufferOffset = 0;                                                                                         \
            _size = 0;                                                                                                 \
            return false;                                                                                              \
        }                                                                                                              \
        _data = multiValueWriter.getData();                                                                            \
        _bufferOffset = (char *)_data - buffer;                                                                        \
        return true;                                                                                                   \
    }                                                                                                                  \
    ha3::M##name *ha3::M##name::create(CavaCtx *ctx, uint32_t len) {                                                   \
        if (len <= 0) {                                                                                                \
            ctx->exception = ExceptionCode::EXC_INVALID_ARGUMENT;                                                      \
            return NULL;                                                                                               \
        }                                                                                                              \
        auto ret = (ha3::M##name *)suez::turing::SuezCavaAllocUtil::alloc(ctx, sizeof(M##name));                       \
        if (unlikely(ret == NULL)) {                                                                                   \
            return NULL;                                                                                               \
        }                                                                                                              \
        if (!ret->init(ctx, len)) {                                                                                    \
            return NULL;                                                                                               \
        }                                                                                                              \
        return ret;                                                                                                    \
    }

MULTI_VALUE_CREATE(Int8, cava::byte)
MULTI_VALUE_CREATE(Int16, short)
MULTI_VALUE_CREATE(Int32, int)
MULTI_VALUE_CREATE(Int64, long)
MULTI_VALUE_CREATE(UInt8, ubyte)
MULTI_VALUE_CREATE(UInt16, ushort)
MULTI_VALUE_CREATE(UInt32, uint)
MULTI_VALUE_CREATE(UInt64, ulong)
MULTI_VALUE_CREATE(Float, float)
MULTI_VALUE_CREATE(Double, double)

MChar *MChar::create(CavaCtx *ctx, CString *str) {
    if (unlikely(str == NULL)) {
        ctx->exception = ExceptionCode::EXC_INVALID_ARGUMENT;
        return NULL;
    }
    size_t bufLen = MultiValueFormatter::calculateBufferLen(str->size(), 1);
    char *buffer = (char *)suez::turing::SuezCavaAllocUtil::alloc(ctx, bufLen);
    if (unlikely(buffer == NULL)) {
        return NULL;
    }
    MultiValueFormatter::formatToBuffer(str->data(), str->size(), buffer, bufLen);
    return (MChar *)buffer;
}

int MChar::compare(CavaCtx *ctx, CString *str) {
    if (unlikely(str == NULL)) {
        ctx->exception = ExceptionCode::EXC_INVALID_ARGUMENT;
        return 0;
    }
    MultiChar mc;
    if (!TransCavaValue2CppValue(this, mc)) {
        assert(false);
    }
    size_t lenMChar = mc.size();
    size_t lenStr = str->size();
    size_t lenMin = std::min(lenMChar, lenStr);
    int ret = memcmp(mc.data(), str->data(), lenMin);
    if (ret == 0 && lenMChar != lenStr) {
        return lenMChar < lenStr ? -1 : 1;
    } else {
        return ret;
    }
}

CString *MChar::toString(CavaCtx *ctx) { return toCString(ctx); }

CString *MChar::toCString(CavaCtx *ctx) {
    if (unlikely(this == NULL)) {
        return NULL;
    }
    MultiChar mc;
    TransCavaValue2CppValue(this, mc); // assert true;
    StringView str(mc.data(), mc.size());
    return suez::turing::SuezCavaAllocUtil::allocCString(ctx, &str);
}

MString::MString(MultiString *val) { reset(val); }

MChar *MString::getWithoutCheck(CavaCtx *ctx, uint index) {
    assert(_offsetAddr != nullptr && "invalid offset attr for get op");
    assert(_offsetItemLen != 0 && "invalid offset item len for get op");
    uint32_t offset = MultiValueFormatter::getOffset(_offsetAddr, _offsetItemLen, index);
    return (MChar *)(_offsetAddr + _offsetItemLen * _size + offset);
}

uint MString::size(CavaCtx *ctx) { return _size; }

MString *MString::create(CavaCtx *ctx, CavaArrayType<CString *> *strArray) {
    if (unlikely(strArray == NULL)) {
        ctx->exception = ExceptionCode::EXC_INVALID_ARGUMENT;
        return NULL;
    }
    size_t length = strArray->length;
    StringView array[length];
    for (size_t i = 0; i < strArray->length; ++i) {
        CString *str = strArray->getData()[i];
        if (str == nullptr) {
            ctx->exception = ExceptionCode::EXC_INVALID_ARGUMENT;
            return NULL;
        }
        array[i] = StringView(str->data(), str->size());
    }

    size_t offsetLen = 0;
    size_t bufLen = MultiValueFormatter::calculateMultiStringBufferLen(array, length, offsetLen);
    char *buffer = (char *)suez::turing::SuezCavaAllocUtil::alloc(ctx, bufLen);
    if (unlikely(buffer == NULL)) {
        return NULL;
    }
    MultiValueFormatter::formatMultiStringToBuffer(buffer, bufLen, offsetLen, &array[0], length);
    MultiString multiString(buffer);
    auto *mString = SuezCavaAllocUtil::createNativeObject<MString>(ctx);
    mString->reset(&multiString);
    return mString;
}

} // namespace ha3
