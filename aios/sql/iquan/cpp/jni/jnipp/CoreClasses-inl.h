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

#include "Meta.h"
#include "jni.h"

namespace iquan {

extern JNIEnv *getIquanJNIEnv();

namespace impl {

template <typename JC, typename... Args>
static LocalRef<JC> newInstance(Args &&...args) {
    static auto cls = JC::javaClass();
    // the constructor return type should be JC instead of JC::jtype, cause
    // jtype may be jobject and lose type information
    static auto constructor =
        cls->template getConstructor<JC(jnipp_remove_ref_t<typename std::decay<Args>::type>...)>();
    return cls->newObject(constructor, args...);
}

template <typename JC, typename C, typename... Args>
static LocalRef<JC> newInstanceWithClassLoader(C clazz, Args &&...args) {
    // the constructor return type should be JC instead of JC::jtype, cause
    // jtype may be jobject and lose type information
    static auto constructor =
        clazz->template getConstructor<JC(jnipp_remove_ref_t<typename std::decay<Args>::type>...)>();
    return clazz->newObject(constructor, args...);
}

} // namespace impl

namespace util {
inline bool isSameObject(AliasRef<jobject> lhs, AliasRef<jobject> rhs) {
    auto env = getIquanJNIEnv();
    return env->IsSameObject(lhs.get(), rhs.get()) == JNI_TRUE;
}
} // namespace util

template <typename T, typename Base, typename JniType>
AliasRef<jclass> JavaClass<T, Base, JniType>::javaClass() {
    static GlobalRef<jclass> clazz = util::findClassGlobal(JTypeTraits<T>::baseName());
    return {clazz};
}

template <typename F>
inline JConstructor<F> JClass::getConstructor() const {
    return getConstructor<F>(JMethodTraitsFromCxx<F>::constructor_descriptor());
}

template <typename F>
inline JConstructor<F> JClass::getConstructor(const std::string &signature) const {
    return getMethod<F>("<init>", signature);
}

template <typename F>
inline JMethod<F> JClass::getMethod(const std::string &name) const {
    return getMethod<F>(name, JMethodTraitsFromCxx<F>::descriptor());
}

template <typename F>
inline JMethod<F> JClass::getMethod(const std::string &name, const std::string &signature) const {
    auto methodId = util::getMethodID(self(), name, signature);
    return JMethod<F> {methodId};
}

template <typename F>
inline JMethod<F> JClass::getStaticMethod(const std::string &name) const {
    return getStaticMethod<F>(name, JMethodTraitsFromCxx<F>::descriptor());
}

template <typename F>
inline JMethod<F> JClass::getStaticMethod(const std::string &name, const std::string &signature) const {
    auto methodId = util::getStaticMethodID(self(), name, signature);
    return JMethod<F> {methodId};
}

template <typename R, typename... Params, typename... Args>
LocalRef<R> JClass::newObject(JConstructor<R(Params...)> constructor, Args &&...args) const {
    auto env = getIquanJNIEnv();
    auto jobj = env->NewObject(
        self(), constructor.getId(), impl::callToJni(Convert<typename std::decay<Params>::type>::toCall(args))...);
    if (!jobj) {
        transJniException();
    }
    return {static_cast<JType<R>>(jobj)};
}

inline size_t JArray::size() const noexcept {
    JNIEnv *env = getIquanJNIEnv();
    return env->GetArrayLength(self());
}

template <typename T>
std::string JObjectArrayT<T>::javaDescriptor() {
    return "[" + JTypeTraits<T>::descriptor();
}

template <typename T>
std::string JObjectArrayT<T>::baseName() {
    return JObjectArrayT<T>::javaDescriptor();
}

template <typename T>
LocalRef<JObjectArrayT<T>> JObjectArrayT<T>::newArray(size_t count) {
    auto env = getIquanJNIEnv();
    auto array = env->NewObjectArray(count, CType<T>::javaClass().get(), nullptr);
    if (!array) {
        transJniException();
    }
    return {array};
}

template <typename T>
void JObjectArrayT<T>::setElement(size_t idx, AliasRef<T> value) {
    auto env = getIquanJNIEnv();
    env->SetObjectArrayElement(self(), idx, value.get());
    transJniException();
}

template <typename T>
LocalRef<T> JObjectArrayT<T>::getElement(size_t idx) {
    auto env = getIquanJNIEnv();
    auto elem = env->GetObjectArrayElement(self(), idx);
    transJniException();
    return {static_cast<JType<T>>(elem)};
}

template <typename jtypeArray>
std::string JPrimitiveArray<jtypeArray>::javaDescriptor() {
    return JTypeTraits<jtypeArray>::descriptor();
}

template <typename jtypeArray>
std::string JPrimitiveArray<jtypeArray>::baseName() {
    return JPrimitiveArray::javaDescriptor();
}

} // namespace iquan
