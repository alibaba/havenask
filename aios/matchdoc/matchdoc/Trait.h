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
#include <type_traits>

namespace matchdoc {

typedef std::string VariableType;

template <typename T>
struct ConstructTypeTraits {
    static constexpr bool NeedConstruct() { return !std::is_trivially_default_constructible<T>::value; }
};

template <typename T>
struct DestructTypeTraits {
    static constexpr bool NeedDestruct() { return !std::is_trivially_destructible<T>::value; }
};

} // namespace matchdoc

// TODO: remove macros below
#define NOT_SUPPORT_SERIZLIE_TYPE(T)
#define NOT_SUPPORT_CLONE_TYPE(T)
#define DECLARE_NO_NEED_DESTRUCT_TYPE(T)
