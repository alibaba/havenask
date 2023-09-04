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

#include "CoreClasses.h"
#include "References.h"
#include "jni.h"

namespace iquan {

// 目前，需要配合使用callToJni(Convert::toCall)
// 对于std::string, toCall只能返回LocalRef<jstring>
// 它不能返回jstring, 因为那要么造成jstring被提早释放，要么是过迟释放
// 现在还没有搞清楚，callToJni接受move参数后，为何就是在jni函数后释放

namespace impl {
template <typename T>
inline T callToJni(T t) {
    return t;
}

template <typename T>
inline JType<T> callToJni(LocalRef<T> &&ref) {
    return ref.get();
}
} // namespace impl

template <typename T, typename Enabled = void>
struct Convert;

template <typename T>
struct Convert<T, typename std::enable_if<IsJniPrimitive<T>::value, void>::type> {
    using type = T;
    static type toCall(T t) {
        return t;
    }
};

template <>
struct Convert<void> {
    using type = void;
};

template <>
struct Convert<bool> {
    using type = jboolean;
    static type toCall(bool t) {
        return t;
    }
};

template <typename T>
struct Convert<T, typename std::enable_if<IsJniReferenceOrWrapper<T>::value, void>::type> {
    using type = CType<T>;
    using jtype = JType<T>;
    template <typename U>
    static jtype toCall(U t) {
        static_assert(IsBaseJniOf<T, U>::value, "cannot convert");
        return t;
    }
    template <typename U, typename Alloc>
    static jtype toCall(const StrongRef<U, Alloc> &t) {
        static_assert(IsBaseJniOf<T, U>::value, "cannot convert");
        return t.get();
    }
    template <typename U, typename Alloc>
    static jtype toCall(StrongRef<U, Alloc> &&t) {
        static_assert(IsBaseJniOf<T, U>::value, "cannot convert");
        return t.get();
    }
    template <typename U>
    static jtype toCall(AliasRef<U> t) {
        static_assert(IsBaseJniOf<T, U>::value, "cannot convert");
        return t.get();
    }
};

template <>
struct Convert<std::string> {
    using type = jstring;
    static LocalRef<jstring> toCall(const std::string &s) {
        return JString::fromStdString(s);
    }
    static LocalRef<jstring> toCall(std::string &&s) {
        return JString::fromStdString(s);
    }
};

template <>
struct Convert<const char *> {
    using type = jstring;
    static LocalRef<jstring> toCall(const char *s) {
        return JString::fromStdString(s);
    }
};

template <typename F>
struct JniSigTypeT;

template <typename R, typename... Args>
struct JniSigTypeT<R(Args...)> {
    using ret_type = typename Convert<typename std::decay<R>::type>::type;
    using type = ret_type(typename Convert<typename std::decay<Args>::type>::type...);
};

template <typename F>
using JniSigType = typename JniSigTypeT<F>::type;

} // namespace iquan
