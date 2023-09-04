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
#include "suez/turing/expression/cava/impl/CommonRefManager.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>

#include "autil/LongHashValue.h"
#include "autil/MultiValueType.h"
#include "matchdoc/CommonDefine.h"
#include "matchdoc/Reference.h"
#include "suez/turing/expression/provider/ProviderBase.h"

class CavaCtx;

using namespace std;
using namespace autil;
using namespace suez::turing;
namespace ha3 {

matchdoc::ReferenceBase *CommonRefManager::require(CavaCtx *ctx, const std::string &name) {
    if (!_enableRequire) {
        return NULL;
    }

    auto variableRef = const_cast<matchdoc::ReferenceBase *>(_provider->requireAttributeWithoutType(name, false));
    if (!variableRef) {
        variableRef = _provider->findVariableReferenceWithoutType(name);
    }
    return variableRef;
}

matchdoc::ReferenceBase *
CommonRefManager::declare(CavaCtx *ctx, const std::string &name, ha3::FieldRefType type, bool serialize) {
    if (!_enableDeclare) {
        return NULL;
    }

#define DECLARE_VARIABLE(FRT_Type, Type)                                                                               \
    case ha3::FRT_Type: {                                                                                              \
        auto ref = _provider->declareVariable<Type>(name, serialize ? SL_VARIABLE : SL_NONE);                          \
        return ref;                                                                                                    \
    }
    switch (type) {
        DECLARE_VARIABLE(FRT_Int8, int8_t)
        DECLARE_VARIABLE(FRT_UInt8, uint8_t)
        DECLARE_VARIABLE(FRT_Int16, int16_t)
        DECLARE_VARIABLE(FRT_UInt16, uint16_t)
        DECLARE_VARIABLE(FRT_Int32, int32_t)
        DECLARE_VARIABLE(FRT_UInt32, uint32_t)
        DECLARE_VARIABLE(FRT_Int64, int64_t)
        DECLARE_VARIABLE(FRT_UInt64, uint64_t)
        DECLARE_VARIABLE(FRT_Float, float)
        DECLARE_VARIABLE(FRT_Double, double)

        DECLARE_VARIABLE(FRT_MInt8, MultiInt8)
        DECLARE_VARIABLE(FRT_MUInt8, MultiUInt8)
        DECLARE_VARIABLE(FRT_MInt16, MultiInt16)
        DECLARE_VARIABLE(FRT_MUInt16, MultiUInt16)
        DECLARE_VARIABLE(FRT_MInt32, MultiInt32)
        DECLARE_VARIABLE(FRT_MUInt32, MultiUInt32)
        DECLARE_VARIABLE(FRT_MInt64, MultiInt64)
        DECLARE_VARIABLE(FRT_MUInt64, MultiUInt64)
        DECLARE_VARIABLE(FRT_MFloat, MultiFloat)
        DECLARE_VARIABLE(FRT_MDouble, MultiDouble)

        DECLARE_VARIABLE(FRT_MChar, MultiChar)
        DECLARE_VARIABLE(FRT_MString, MultiString)
    default:
        break;
    }
    return NULL;
}

matchdoc::ReferenceBase *CommonRefManager::find(CavaCtx *ctx, const std::string &name) {
    return const_cast<matchdoc::ReferenceBase *>(_provider->findVariableReferenceWithoutType(name));
}

} // namespace ha3
