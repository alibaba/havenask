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

#include <memory>

#include "autil/NoCopyable.h"
#include "matchdoc/Reference.h"

namespace matchdoc {

// TODO: design a real type system for matchdoc
//       decouple type meta and read/write from Reference
class ReferenceTypesWrapper : public autil::NoCopyable {
public:
    ~ReferenceTypesWrapper();

private:
    ReferenceTypesWrapper();

public:
    static ReferenceTypesWrapper *getInstance();

public:
    template <typename T>
    void registerType() {
        registerTypeImpl(std::make_unique<Reference<T>>());
    }

    template <typename T>
    void unregisterType() {
        return unregisterTypeImpl(typeid(T).name());
    }

    const ReferenceBase *lookupType(const std::string &typeName) const;

private:
    void registerTypeImpl(std::unique_ptr<ReferenceBase> refBase);
    void unregisterTypeImpl(const std::string &typeName);
    void registerBuiltinTypes();

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
};

namespace __detail {
template <typename T>
struct TypeRegister {
    TypeRegister() { ReferenceTypesWrapper::getInstance()->registerType<T>(); }
};

} // namespace __detail

#define REGISTER_MATCHDOC_TYPE(T) static matchdoc::__detail::TypeRegister<T> __regist##T;

} // namespace matchdoc

// 仅用于插件so里自定义类型，其他情况下不需要
#define __MD_REGISTER_TYPE(T)                                                                                          \
    __attribute__((constructor)) void registerType() {                                                                 \
        matchdoc::ReferenceTypesWrapper::getInstance()->registerType<T>();                                             \
    }

#define __MD_UNREGISTER_TYPE(T)                                                                                        \
    __attribute__((destructor)) void unregisterType() {                                                                \
        matchdoc::ReferenceTypesWrapper::getInstance()->unregisterType<T>();                                           \
    }

#define DECLARE_NEW_TYPE(T)                                                                                            \
    __MD_REGISTER_TYPE(T);                                                                                             \
    __MD_UNREGISTER_TYPE(T)
