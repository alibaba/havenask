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

#include "Convert.h"
#include "Exceptions.h"
#include "jni.h"

namespace iquan {

extern JNIEnv *getIquanJNIEnv();

template <typename... Params>
template <typename... Args>
inline void JMethod<void(Params...)>::operator()(AliasRef<jobject> self, Args &&...args) {
    JNIEnv *env = getIquanJNIEnv();
    env->CallVoidMethod(
        self.get(), getId(), impl::callToJni(Convert<typename std::decay<Params>::type>::toCall(args))...);
    transJniException();
}

#define DEFINE_PRIMITIVE_METHOD_CALL(TYPE, METHOD)                                                                     \
    template <typename... Params>                                                                                      \
    template <typename... Args>                                                                                        \
    inline TYPE JMethod<TYPE(Params...)>::operator()(AliasRef<jobject> self, Args &&...args) {                         \
        JNIEnv *env = getIquanJNIEnv();                                                                                \
        auto result = env->Call##METHOD##Method(                                                                       \
            self.get(), getId(), impl::callToJni(Convert<typename std::decay<Params>::type>::toCall(args))...);        \
        transJniException();                                                                                           \
        return (TYPE)result;                                                                                           \
    }

DEFINE_PRIMITIVE_METHOD_CALL(jboolean, Boolean)
DEFINE_PRIMITIVE_METHOD_CALL(jbyte, Byte)
DEFINE_PRIMITIVE_METHOD_CALL(jchar, Char)
DEFINE_PRIMITIVE_METHOD_CALL(jshort, Short)
DEFINE_PRIMITIVE_METHOD_CALL(jint, Int)
DEFINE_PRIMITIVE_METHOD_CALL(jlong, Long)
DEFINE_PRIMITIVE_METHOD_CALL(jfloat, Float)
DEFINE_PRIMITIVE_METHOD_CALL(jdouble, Double)

DEFINE_PRIMITIVE_METHOD_CALL(bool, Boolean)

#undef DEFINE_PRIMITIVE_METHOD_CALL

template <typename... Params>
template <typename... Args>
inline std::string JMethod<std::string(Params...)>::operator()(AliasRef<jobject> self, Args &&...args) {
    JNIEnv *env = getIquanJNIEnv();
    jstring js = (jstring)env->CallObjectMethod(
        self.get(), getId(), impl::callToJni(Convert<typename std::decay<Params>::type>::toCall(args))...);
    transJniException();
    return LocalRef<jstring> { js }
    ->toStdString();
}

template <typename R, typename... Params>
template <typename... Args>
inline auto JMethod<R(Params...)>::operator()(AliasRef<jobject> self, Args &&...args) -> LocalRef<cret> {
    static_assert(sizeof...(Params) == sizeof...(Args), "Params and Args number must match");
    JNIEnv *env = getIquanJNIEnv();
    auto result = env->CallObjectMethod(
        self.get(), getId(), impl::callToJni(Convert<typename std::decay<Params>::type>::toCall(args))...);
    transJniException();
    return {static_cast<jret>(result)};
}

template <typename... Params>
template <typename... Args>
inline void JStaticMethod<void(Params...)>::operator()(AliasRef<jclass> cls, Args &&...args) {
    const auto env = getIquanJNIEnv();
    env->CallStaticVoidMethod(
        cls.get(), getId(), impl::callToJni(Convert<typename std::decay<Params>::type>::toCall(args))...);
    transJniException();
}

#define DEFINE_PRIMITIVE_STATIC_METHOD_CALL(TYPE, METHOD)                                                              \
    template <typename... Params>                                                                                      \
    template <typename... Args>                                                                                        \
    inline TYPE JStaticMethod<TYPE(Params...)>::operator()(AliasRef<jclass> cls, Args &&...args) {                     \
        JNIEnv *env = getIquanJNIEnv();                                                                                \
        auto result = env->CallStatic##METHOD##Method(                                                                 \
            cls.get(), getId(), impl::callToJni(Convert<typename std::decay<Params>::type>::toCall(args))...);         \
        transJniException();                                                                                           \
        return (TYPE)result;                                                                                           \
    }

DEFINE_PRIMITIVE_STATIC_METHOD_CALL(jboolean, Boolean)
DEFINE_PRIMITIVE_STATIC_METHOD_CALL(jbyte, Byte)
DEFINE_PRIMITIVE_STATIC_METHOD_CALL(jchar, Char)
DEFINE_PRIMITIVE_STATIC_METHOD_CALL(jshort, Short)
DEFINE_PRIMITIVE_STATIC_METHOD_CALL(jint, Int)
DEFINE_PRIMITIVE_STATIC_METHOD_CALL(jlong, Long)
DEFINE_PRIMITIVE_STATIC_METHOD_CALL(jfloat, Float)
DEFINE_PRIMITIVE_STATIC_METHOD_CALL(jdouble, Double)

DEFINE_PRIMITIVE_STATIC_METHOD_CALL(bool, Boolean)

#undef DEFINE_PRIMITIVE_STATIC_METHOD_CALL

template <typename... Params>
template <typename... Args>
inline std::string JStaticMethod<std::string(Params...)>::operator()(AliasRef<jclass> cls, Args &&...args) {
    const auto env = getIquanJNIEnv();
    jstring js = (jstring)env->CallStaticObjectMethod(
        cls.get(), getId(), impl::callToJni(Convert<typename std::decay<Params>::type>::toCall(args))...);
    transJniException();
    return LocalRef<jstring> { js }
    ->toStdString();
}

template <typename R, typename... Params>
template <typename... Args>
inline auto JStaticMethod<R(Params...)>::operator()(AliasRef<jclass> cls, Args &&...args) -> LocalRef<cret> {
    const auto env = getIquanJNIEnv();
    auto result = env->CallStaticObjectMethod(
        cls.get(), getId(), impl::callToJni(Convert<typename std::decay<Params>::type>::toCall(args))...);
    transJniException();
    return {static_cast<jret>(result)};
}

template <typename T>
inline std::string javaDescriptor() {
    return JTypeTraits<T>::descriptor();
}

template <typename Head, typename Elem, typename... Tail>
inline std::string javaDescriptor() {
    return javaDescriptor<Head>() + javaDescriptor<Elem, Tail...>();
}

template <typename R>
inline std::string jmethodDescriptor() {
    return "()" + javaDescriptor<R>();
}

template <typename R, typename Arg1, typename... Args>
inline std::string jmethodDescriptor() {
    return "(" + javaDescriptor<Arg1, Args...>() + ")" + javaDescriptor<R>();
}

template <typename R, typename... Args>
inline std::string JMethodTraits<R(Args...)>::descriptor() {
    return jmethodDescriptor<R, Args...>();
}

template <typename R, typename... Args>
inline std::string JMethodTraits<R(Args...)>::constructor_descriptor() {
    return jmethodDescriptor<void, Args...>();
}

template <typename R, typename... Args>
struct JMethodTraitsFromCxx<R(Args...)> : public JMethodTraits<JniSigType<R(Args...)>> {};

} // namespace iquan
