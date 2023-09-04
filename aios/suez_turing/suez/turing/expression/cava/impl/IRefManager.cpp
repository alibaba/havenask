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
#include "suez/turing/expression/cava/impl/IRefManager.h"

#include <iosfwd>

#include "autil/CommonMacros.h"
#include "cava/runtime/CString.h"
#include "matchdoc/Reference.h"
#include "matchdoc/ValueType.h"
#include "suez/turing/expression/cava/common/SuezCavaAllocator.h"

class CavaCtx;

using namespace std;

namespace ha3 {

IRefManager::IRefManager() {}

IRefManager::~IRefManager() {}

#define CODE_SNIPPET(type, bt_type, multi, isConst)                                                                    \
    if (!ref) {                                                                                                        \
        return nullptr;                                                                                                \
    }                                                                                                                  \
    auto valueType = ref->getValueType();                                                                              \
    if (unlikely(valueType.getBuiltinType() != bt_type || valueType.isMultiValue() != multi)) {                        \
        return nullptr;                                                                                                \
    }                                                                                                                  \
    auto ret = suez::turing::SuezCavaAllocUtil::allocRef<type##Ref>(ctx, ref, isConst);

#define REQUIRE_CODE_SNIPPET(str, type, bt_type, multi, isConst)                                                       \
    auto ref = require(ctx, str);                                                                                      \
    CODE_SNIPPET(type, bt_type, multi, true)

#define DECLARE_CODE_SNIPPET(str, type, bt_type, multi, isConst)                                                       \
    auto ref = declare(ctx, str, FRT_##type, serialize);                                                               \
    CODE_SNIPPET(type, bt_type, multi, false)

#define FIND_CODE_SNIPPET(str, type, bt_type, multi, isConst)                                                          \
    auto ref = find(ctx, str);                                                                                         \
    CODE_SNIPPET(type, bt_type, multi, true)

#define VARIABLE_FUNC_IMPL(type, bt_type, multi)                                                                       \
    type##Ref *IRefManager::require##type(CavaCtx *ctx, cava::lang::CString *name) {                                   \
        if (unlikely(name == nullptr)) {                                                                               \
            return nullptr;                                                                                            \
        }                                                                                                              \
        const std::string &str = name->getStr();                                                                       \
        REQUIRE_CODE_SNIPPET(str, type, bt_type, multi, require);                                                      \
        return ret;                                                                                                    \
    }                                                                                                                  \
    type##Ref *IRefManager::declare##type(CavaCtx *ctx, cava::lang::CString *name, bool serialize) {                   \
        if (unlikely(name == nullptr)) {                                                                               \
            return nullptr;                                                                                            \
        }                                                                                                              \
        const std::string &str = name->getStr();                                                                       \
        DECLARE_CODE_SNIPPET(str, type, bt_type, multi, declare);                                                      \
        return ret;                                                                                                    \
    }                                                                                                                  \
    type##Ref *IRefManager::find##type(CavaCtx *ctx, cava::lang::CString *name) {                                      \
        if (unlikely(name == nullptr)) {                                                                               \
            return nullptr;                                                                                            \
        }                                                                                                              \
        const std::string &str = name->getStr();                                                                       \
        FIND_CODE_SNIPPET(str, type, bt_type, multi, find);                                                            \
        return ret;                                                                                                    \
    }

VARIABLE_FUNC_IMPL(Int8, matchdoc::bt_int8, false);
VARIABLE_FUNC_IMPL(Int16, matchdoc::bt_int16, false);
VARIABLE_FUNC_IMPL(Int32, matchdoc::bt_int32, false);
VARIABLE_FUNC_IMPL(Int64, matchdoc::bt_int64, false);
VARIABLE_FUNC_IMPL(UInt8, matchdoc::bt_uint8, false);
VARIABLE_FUNC_IMPL(UInt16, matchdoc::bt_uint16, false);
VARIABLE_FUNC_IMPL(UInt32, matchdoc::bt_uint32, false);
VARIABLE_FUNC_IMPL(UInt64, matchdoc::bt_uint64, false);
VARIABLE_FUNC_IMPL(Float, matchdoc::bt_float, false);
VARIABLE_FUNC_IMPL(Double, matchdoc::bt_double, false);
VARIABLE_FUNC_IMPL(MChar, matchdoc::bt_string, false);

VARIABLE_FUNC_IMPL(MInt8, matchdoc::bt_int8, true);
VARIABLE_FUNC_IMPL(MInt16, matchdoc::bt_int16, true);
VARIABLE_FUNC_IMPL(MInt32, matchdoc::bt_int32, true);
VARIABLE_FUNC_IMPL(MInt64, matchdoc::bt_int64, true);
VARIABLE_FUNC_IMPL(MUInt8, matchdoc::bt_uint8, true);
VARIABLE_FUNC_IMPL(MUInt16, matchdoc::bt_uint16, true);
VARIABLE_FUNC_IMPL(MUInt32, matchdoc::bt_uint32, true);
VARIABLE_FUNC_IMPL(MUInt64, matchdoc::bt_uint64, true);
VARIABLE_FUNC_IMPL(MFloat, matchdoc::bt_float, true);
VARIABLE_FUNC_IMPL(MDouble, matchdoc::bt_double, true);
VARIABLE_FUNC_IMPL(MString, matchdoc::bt_string, true);

} // namespace ha3
