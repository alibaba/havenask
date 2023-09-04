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

#include "cava/common/Common.h"
#include "suez/turing/expression/cava/impl/CavaMultiValueTyped.h"

class CavaCtx;
namespace matchdoc {
class ReferenceBase;
} // namespace matchdoc

namespace ha3 {
class DoubleRef;
class FloatRef;
class Int16Ref;
class Int32Ref;
class Int64Ref;
class Int8Ref;
class MCharRef;
class MDoubleRef;
class MFloatRef;
class MInt16Ref;
class MInt32Ref;
class MInt64Ref;
class MInt8Ref;
class MStringRef;
class MUInt16Ref;
class MUInt32Ref;
class MUInt64Ref;
class MUInt8Ref;
class MatchDoc;
class UInt16Ref;
class UInt32Ref;
class UInt64Ref;
class UInt8Ref;

enum FieldRefType {
    FRT_Undefined = 0,
    FRT_Int8 = 1,
    FRT_UInt8 = 2,
    FRT_Int16 = 3,
    FRT_UInt16 = 4,
    FRT_Int32 = 5,
    FRT_UInt32 = 6,
    FRT_Int64 = 7,
    FRT_UInt64 = 8,
    FRT_Float = 9,
    FRT_Double = 10,

    FRT_MInt8 = 11,
    FRT_MUInt8 = 12,
    FRT_MInt16 = 13,
    FRT_MUInt16 = 14,
    FRT_MInt32 = 15,
    FRT_MUInt32 = 16,
    FRT_MInt64 = 17,
    FRT_MUInt64 = 18,
    FRT_MFloat = 19,
    FRT_MDouble = 20,

    FRT_MChar = 21,   // isMulti false
    FRT_MString = 22, // isMulti true
    // FRT_String,
    // FRT_SimpleMatchdata,
    // FRT_Matchdata,
};

#define SINGLE_REFERENCE_DECLARE(name, type)                                                                           \
    class name##Ref {                                                                                                  \
    public:                                                                                                            \
        name##Ref(matchdoc::ReferenceBase *ref, bool isConst) : _ref(ref), _isConst(isConst) {}                        \
        ~name##Ref() {}                                                                                                \
                                                                                                                       \
    public:                                                                                                            \
        type get(CavaCtx *ctx, MatchDoc *doc);                                                                         \
        void set(CavaCtx *ctx, MatchDoc *doc, type val);                                                               \
                                                                                                                       \
    public:                                                                                                            \
        bool isMulti() { return false; }                                                                               \
        bool isConst() { return _isConst; }                                                                            \
                                                                                                                       \
    private:                                                                                                           \
        matchdoc::ReferenceBase *_ref;                                                                                 \
        bool _isConst;                                                                                                 \
    };

#define MULTI_REFERENCE_DECLARE(name, type)                                                                            \
    class name##Ref {                                                                                                  \
    public:                                                                                                            \
        name##Ref(matchdoc::ReferenceBase *ref, bool isConst) : _ref(ref), _isConst(isConst) {}                        \
        ~name##Ref() {}                                                                                                \
                                                                                                                       \
    public:                                                                                                            \
        type *get(CavaCtx *ctx, MatchDoc *doc);                                                                        \
        void set(CavaCtx *ctx, MatchDoc *doc, type *val);                                                              \
                                                                                                                       \
    public:                                                                                                            \
        bool isMulti() { return true; }                                                                                \
        bool isConst() { return _isConst; }                                                                            \
                                                                                                                       \
    private:                                                                                                           \
        matchdoc::ReferenceBase *_ref;                                                                                 \
        type _val;                                                                                                     \
        bool _isConst;                                                                                                 \
    };

SINGLE_REFERENCE_DECLARE(Int8, cava::byte);
SINGLE_REFERENCE_DECLARE(Int16, short);
SINGLE_REFERENCE_DECLARE(Int32, int);
SINGLE_REFERENCE_DECLARE(Int64, long);
SINGLE_REFERENCE_DECLARE(UInt8, ubyte);
SINGLE_REFERENCE_DECLARE(UInt16, ushort);
SINGLE_REFERENCE_DECLARE(UInt32, uint);
SINGLE_REFERENCE_DECLARE(UInt64, ulong);
SINGLE_REFERENCE_DECLARE(Float, float);
SINGLE_REFERENCE_DECLARE(Double, double);
SINGLE_REFERENCE_DECLARE(MChar, MChar *);

MULTI_REFERENCE_DECLARE(MInt8, MInt8);
MULTI_REFERENCE_DECLARE(MInt16, MInt16);
MULTI_REFERENCE_DECLARE(MInt32, MInt32);
MULTI_REFERENCE_DECLARE(MInt64, MInt64);
MULTI_REFERENCE_DECLARE(MUInt8, MUInt8);
MULTI_REFERENCE_DECLARE(MUInt16, MUInt16);
MULTI_REFERENCE_DECLARE(MUInt32, MUInt32);
MULTI_REFERENCE_DECLARE(MUInt64, MUInt64);
MULTI_REFERENCE_DECLARE(MFloat, MFloat);
MULTI_REFERENCE_DECLARE(MDouble, MDouble);
MULTI_REFERENCE_DECLARE(MString, MString);

} // namespace ha3
