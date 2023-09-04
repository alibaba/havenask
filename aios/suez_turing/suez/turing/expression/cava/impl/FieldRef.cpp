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
#include "suez/turing/expression/cava/impl/FieldRef.h"

#include <iosfwd>
#include <new>
#include <stdint.h>
#include <string.h>

#include "autil/CommonMacros.h"
#include "autil/LongHashValue.h"
#include "autil/MultiValueFormatter.h"
#include "autil/MultiValueType.h"
#include "cava/common/ErrorDef.h"
#include "cava/runtime/CavaCtx.h"
#include "matchdoc/Reference.h"
#include "suez/turing/expression/cava/common/CavaFieldTypeHelper.h"
#include "suez/turing/expression/cava/common/SuezCavaAllocator.h"
#include "suez/turing/expression/cava/impl/MatchDoc.h"

namespace matchdoc {
class MatchDoc;
} // namespace matchdoc

using namespace std;
using namespace autil;
using namespace cava;

namespace ha3 {

#define SINGLE_VALUE_REF_IMPL(name, type)                                                                              \
    type ha3::name##Ref::get(CavaCtx *ctx, ha3::MatchDoc *doc) {                                                       \
        if (unlikely(doc == NULL)) {                                                                                   \
            ctx->exception = 1;                                                                                        \
            return 0;                                                                                                  \
        }                                                                                                              \
        auto typedRef = (matchdoc::Reference<type> *)_ref;                                                             \
        return typedRef->get(*(matchdoc::MatchDoc *)doc);                                                              \
    }                                                                                                                  \
    void ha3::name##Ref::set(CavaCtx *ctx, ha3::MatchDoc *doc, type val) {                                             \
        if (unlikely(doc == NULL)) {                                                                                   \
            ctx->exception = 1;                                                                                        \
            return;                                                                                                    \
        }                                                                                                              \
        auto typedRef = (matchdoc::Reference<type> *)_ref;                                                             \
        typedRef->set(*(matchdoc::MatchDoc *)doc, val);                                                                \
    }

#define MULTI_VALUE_REF_IMPL(name, type)                                                                               \
    name *ha3::name##Ref::get(CavaCtx *ctx, ha3::MatchDoc *doc) {                                                      \
        if (unlikely(doc == NULL)) {                                                                                   \
            ctx->exception = 1;                                                                                        \
            return NULL;                                                                                               \
        }                                                                                                              \
        typedef matchdoc::Reference<MultiValueType<type>> MRefType;                                                    \
        auto mRef = (MRefType *)_ref;                                                                                  \
        auto elem = mRef->getPointer(*(matchdoc::MatchDoc *)doc);                                                      \
        name *ret = &_val;                                                                                             \
        new (ret) ha3::name(elem);                                                                                     \
        if (isConst()) {                                                                                               \
            ret->setConst();                                                                                           \
        }                                                                                                              \
        return ret;                                                                                                    \
    }                                                                                                                  \
    void ha3::name##Ref::set(CavaCtx *ctx, ha3::MatchDoc *doc, name *val) {                                            \
        if (unlikely(val == NULL || doc == NULL)) {                                                                    \
            ctx->exception = 1;                                                                                        \
            return;                                                                                                    \
        }                                                                                                              \
        typedef matchdoc::Reference<MultiValueType<type>> MRefType;                                                    \
        auto ref = (MRefType *)_ref;                                                                                   \
        auto &data = ref->getReference(*(matchdoc::MatchDoc *)doc);                                                    \
        char *buf = val->getMultiValueBuffer();                                                                        \
        if (likely(!val->isConst())) {                                                                                 \
            data.init(buf);                                                                                            \
            return;                                                                                                    \
        }                                                                                                              \
        int size = val->size(ctx) * sizeof(type);                                                                      \
        size += MultiValueFormatter::getEncodedCountFromFirstByte(*(uint8_t *)buf);                                    \
        char *ret = (char *)suez::turing::SuezCavaAllocUtil::alloc(ctx, size);                                         \
        memcpy(ret, buf, size);                                                                                        \
        data.init(ret);                                                                                                \
        return;                                                                                                        \
    }

SINGLE_VALUE_REF_IMPL(Int8, cava::byte);
SINGLE_VALUE_REF_IMPL(Int16, short);
SINGLE_VALUE_REF_IMPL(Int32, int);
SINGLE_VALUE_REF_IMPL(Int64, long);
SINGLE_VALUE_REF_IMPL(UInt8, ubyte);
SINGLE_VALUE_REF_IMPL(UInt16, ushort);
SINGLE_VALUE_REF_IMPL(UInt32, uint);
SINGLE_VALUE_REF_IMPL(UInt64, ulong);
SINGLE_VALUE_REF_IMPL(Float, float);
SINGLE_VALUE_REF_IMPL(Double, double);

MULTI_VALUE_REF_IMPL(MInt8, cava::byte);
MULTI_VALUE_REF_IMPL(MInt16, short);
MULTI_VALUE_REF_IMPL(MInt32, int);
MULTI_VALUE_REF_IMPL(MInt64, long);
MULTI_VALUE_REF_IMPL(MUInt8, ubyte);
MULTI_VALUE_REF_IMPL(MUInt16, ushort);
MULTI_VALUE_REF_IMPL(MUInt32, uint);
MULTI_VALUE_REF_IMPL(MUInt64, ulong);
MULTI_VALUE_REF_IMPL(MFloat, float);
MULTI_VALUE_REF_IMPL(MDouble, double);

ha3::MChar *ha3::MCharRef::get(CavaCtx *ctx, ha3::MatchDoc *doc) {
    if (unlikely(doc == NULL)) {
        ctx->exception = 1;
        return 0;
    }
    auto typedRef = (matchdoc::Reference<MultiChar> *)_ref;
    auto multiChar = typedRef->get(*(matchdoc::MatchDoc *)doc);
    auto *baseAddr = multiChar.getBaseAddress();
    assert(multiChar.isEmptyData() || multiChar.hasEncodedCount());
    if (baseAddr == nullptr) {
        // baseAddr == nullptr is valid resp for empty string
        return (ha3::MChar *)SPECIAL_EMPTY_MCHAR_ADDR;
    }
    return (ha3::MChar *)baseAddr;
}

void ha3::MCharRef::set(CavaCtx *ctx, ha3::MatchDoc *doc, MChar *val) {
    if (unlikely(val == NULL || doc == NULL)) {
        ctx->exception = 1;
        return;
    }
    auto typedRef = (matchdoc::Reference<MultiChar> *)_ref;
    autil::MultiChar multiChar((const char *)val);
    typedRef->set(*(matchdoc::MatchDoc *)doc, multiChar);
}

MString *ha3::MStringRef::get(CavaCtx *ctx, ha3::MatchDoc *doc) {
    if (unlikely(doc == NULL)) {
        ctx->exception = 1;
        return NULL;
    }
    typedef matchdoc::Reference<MultiString> MRefType;
    auto mRef = (MRefType *)_ref;
    auto elem = mRef->getPointer(*(matchdoc::MatchDoc *)doc);
    MString *ret = &_val;
    new (ret) ha3::MString(elem);
    return ret;
}

void ha3::MStringRef::set(CavaCtx *ctx, ha3::MatchDoc *doc, MString *val) {
    if (unlikely(val == NULL || doc == NULL)) {
        ctx->exception = ExceptionCode::EXC_INVALID_ARGUMENT;
        return;
    }
    typedef matchdoc::Reference<MultiString> MRefType;
    auto *ref = (MRefType *)_ref;
    auto &data = ref->getReference(*(matchdoc::MatchDoc *)doc);
    TransCavaValue2CppValue(val, data);
}

} // namespace ha3
