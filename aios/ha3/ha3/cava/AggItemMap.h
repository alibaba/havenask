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

#include <cstdint>
#include <functional>
#include <iosfwd>
#include <system_error>
#include <utility>
#include <unordered_map>

#include "autil/HashAlgorithm.h"
#include "autil/Log.h" // IWYU pragma: keep
#include "autil/MultiValueFormatter.h"
#include "cava/common/Common.h"
#include "cava/runtime/Any.h" // IWYU pragma: keep
#include "suez/turing/expression/cava/common/CavaFieldTypeHelper.h"


class CavaCtx;

namespace ha3 {
class MChar;
}  // namespace ha3

namespace std {

template<>
struct hash<ha3::MChar*> {
    std::size_t operator() (ha3::MChar * const key) const {
        if (key == nullptr) {
            return 0;
        }
        autil::MultiChar mc;
        if (!TransCavaValue2CppValue(key, mc)) {
            assert(false);
        }
        return std::hash<autil::MultiChar>()(mc);
    }
};

template<>
struct equal_to<ha3::MChar *> {
    bool operator() (ha3::MChar * const left, ha3::MChar * const right) const {
        if (left == nullptr || right == nullptr) {
            return left == right;
        }
        autil::MultiChar lhs, rhs;
        if (!TransCavaValue2CppValue(left, lhs)
            || !TransCavaValue2CppValue(right, rhs)) {
            assert(false);
        }
        return std::equal_to<autil::MultiChar>()(lhs, rhs);
    }
};

}


namespace unsafe {

#define DECLARE_AGGITEMMAP_CLASS(name, type)                            \
    class name##AggItemMap                                              \
    {                                                                   \
    public:                                                             \
        name##AggItemMap();                                             \
    public:                                                             \
        static name##AggItemMap *create(CavaCtx *ctx);                  \
    public:                                                             \
        unsafe::Any *get(CavaCtx *ctx, type key);                       \
        void add(CavaCtx *ctx, type key, unsafe::Any *value);           \
        uint size(CavaCtx *ctx);                                        \
        unsafe::Any *begin(CavaCtx *ctx);                               \
        unsafe::Any *next(CavaCtx *ctx);                                \
    private:                                                            \
        typedef std::unordered_map<type, unsafe::Any*> HashMapT;        \
        HashMapT _aggItemMap;                                           \
        HashMapT::iterator _iter;                                       \
    };

DECLARE_AGGITEMMAP_CLASS(Byte, cava::byte);
DECLARE_AGGITEMMAP_CLASS(Short, short);
DECLARE_AGGITEMMAP_CLASS(Int, int);
DECLARE_AGGITEMMAP_CLASS(Long, long);
DECLARE_AGGITEMMAP_CLASS(UByte, ubyte);
DECLARE_AGGITEMMAP_CLASS(UShort, ushort);
DECLARE_AGGITEMMAP_CLASS(UInt, uint);
DECLARE_AGGITEMMAP_CLASS(ULong, ulong);
DECLARE_AGGITEMMAP_CLASS(Float, float);
DECLARE_AGGITEMMAP_CLASS(Double, double);
DECLARE_AGGITEMMAP_CLASS(MChar, ha3::MChar*);

}
