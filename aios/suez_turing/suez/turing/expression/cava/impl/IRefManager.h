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

#include <string>

#include "suez/turing/expression/cava/impl/FieldRef.h"

class CavaCtx;
namespace cava {
namespace lang {
class CString;
} // namespace lang
} // namespace cava
namespace matchdoc {
class ReferenceBase;
} // namespace matchdoc

namespace ha3 {

class IRefManager {
public:
    IRefManager();
    virtual ~IRefManager() = 0;

private:
    IRefManager(const IRefManager &);
    IRefManager &operator=(const IRefManager &);

public:
    virtual matchdoc::ReferenceBase *require(CavaCtx *ctx, const std::string &name) = 0;
    virtual matchdoc::ReferenceBase *
    declare(CavaCtx *ctx, const std::string &name, FieldRefType type, bool serialize) = 0;
    virtual matchdoc::ReferenceBase *find(CavaCtx *ctx, const std::string &name) = 0;

#define VARIABLE_FUNC_HEADER(type)                                                                                     \
    type##Ref *require##type(CavaCtx *ctx, cava::lang::CString *name);                                                 \
    type##Ref *declare##type(CavaCtx *ctx, cava::lang::CString *name, bool serialize);                                 \
    type##Ref *find##type(CavaCtx *ctx, cava::lang::CString *name);

    VARIABLE_FUNC_HEADER(Int8);
    VARIABLE_FUNC_HEADER(Int16);
    VARIABLE_FUNC_HEADER(Int32);
    VARIABLE_FUNC_HEADER(Int64);
    VARIABLE_FUNC_HEADER(UInt8);
    VARIABLE_FUNC_HEADER(UInt16);
    VARIABLE_FUNC_HEADER(UInt32);
    VARIABLE_FUNC_HEADER(UInt64);
    VARIABLE_FUNC_HEADER(Float);
    VARIABLE_FUNC_HEADER(Double);
    VARIABLE_FUNC_HEADER(MChar);

    VARIABLE_FUNC_HEADER(MInt8);
    VARIABLE_FUNC_HEADER(MInt16);
    VARIABLE_FUNC_HEADER(MInt32);
    VARIABLE_FUNC_HEADER(MInt64);
    VARIABLE_FUNC_HEADER(MUInt8);
    VARIABLE_FUNC_HEADER(MUInt16);
    VARIABLE_FUNC_HEADER(MUInt32);
    VARIABLE_FUNC_HEADER(MUInt64);
    VARIABLE_FUNC_HEADER(MFloat);
    VARIABLE_FUNC_HEADER(MDouble);
    VARIABLE_FUNC_HEADER(MString);
};

} // namespace ha3
