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

#include <type_traits>

#include "CoreClasses-fwd.h"
#include "jni.h"

namespace iquan {

template <typename T>
struct IsJniPrimitive : std::integral_constant<bool,
                                               std::is_same<jboolean, T>::value || std::is_same<jbyte, T>::value ||
                                                   std::is_same<jchar, T>::value || std::is_same<jshort, T>::value ||
                                                   std::is_same<jint, T>::value || std::is_same<jlong, T>::value ||
                                                   std::is_same<jfloat, T>::value || std::is_same<jdouble, T>::value> {
};

template <typename T>
struct IsJniReference : std::integral_constant<bool,
                                               std::is_pointer<T>::value &&
                                                   std::is_base_of<typename std::remove_pointer<jobject>::type,
                                                                   typename std::remove_pointer<T>::type>::value> {};

template <typename T, typename U>
struct IsBaseJniReferenceOf
    : std::integral_constant<
          bool,
          std::is_pointer<T>::value && std::is_pointer<U>::value &&
              std::is_base_of<typename std::remove_pointer<T>::type, typename std::remove_pointer<U>::type>::value> {};

template <typename T>
struct IsJniWrapper : std::integral_constant<bool, std::is_base_of<JObject, T>::value> {};

template <typename T>
struct IsJniReferenceOrWrapper : std::integral_constant<bool, IsJniReference<T>::value || IsJniWrapper<T>::value> {};

template <typename T, typename Enable = void>
struct CTypeT;

template <typename T, typename Enable = void>
struct JTypeT;

#define CORE_CLASS_TYPES(ctype, jtype)                                                                                 \
    template <>                                                                                                        \
    struct CTypeT<jtype> {                                                                                             \
        using type = ctype;                                                                                            \
    };
CORE_CLASS_TYPES(JObject, jobject)
CORE_CLASS_TYPES(JClass, jclass)
CORE_CLASS_TYPES(JString, jstring)
CORE_CLASS_TYPES(JThrowable, jthrowable)
CORE_CLASS_TYPES(JObjectArray, jobjectArray)
#undef CORE_CLASS_TYPES

template <typename T>
struct CTypeT<T, typename std::enable_if<std::is_base_of<JObject, T>::value, void>::type> {
    using type = T;
};

template <typename T>
struct JTypeT<T, typename std::enable_if<std::is_base_of<JObject, T>::value, void>::type> {
    using type = typename T::jtype;
};

template <typename T>
struct JTypeT<
    T,
    typename std::enable_if<std::is_base_of<_jobject, typename std::remove_pointer<T>::type>::value, void>::type> {
    using type = T;
};

template <typename T>
using CType = typename CTypeT<T>::type;

template <typename T>
using JType = typename JTypeT<T>::type;

template <typename T, typename U>
struct IsBaseJniOf : std::integral_constant<bool,
                                            IsBaseJniReferenceOf<JType<T>, JType<U>>::value &&
                                                std::is_base_of<CType<T>, CType<U>>::value> {};

} // namespace iquan
