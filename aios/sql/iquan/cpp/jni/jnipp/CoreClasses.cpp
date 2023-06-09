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
#include "iquan/jni/jnipp/CoreClasses.h"

#include "iquan/jni/jnipp/Meta.h"
#include "iquan/jni/jnipp/References.h"
#include "jni.h"

namespace iquan {

extern JNIEnv *getIquanJNIEnv();

namespace util {
LocalRef<jclass> findClassLocal(const std::string &name) {
    JNIEnv *env = getIquanJNIEnv();
    jclass clazz = env->FindClass(name.c_str());
    if (!clazz) {
        transJniException();
    }
    return {clazz};
}

GlobalRef<jclass> findClassGlobal(const std::string &name) { return {findClassLocal(name)}; }

jmethodID getMethodID(jclass clazz, const std::string &name, const std::string &signature) {
    JNIEnv *env = getIquanJNIEnv();
    jmethodID method = env->GetMethodID(clazz, name.c_str(), signature.c_str());
    if (!method) {
        transJniException();
    }
    return method;
}

jmethodID getStaticMethodID(jclass clazz, const std::string &name, const std::string &signature) {
    JNIEnv *env = getIquanJNIEnv();
    jmethodID method = env->GetStaticMethodID(clazz, name.c_str(), signature.c_str());
    if (!method) {
        transJniException();
    }
    return method;
}
} // namespace util

AliasRef<jclass> JObject::javaClass() {
    static GlobalRef<jclass> clazz = util::findClassGlobal(JTypeTraits<jobject>::baseName());
    return {clazz};
}

LocalRef<jclass> JObject::getClass() const {
    JNIEnv *env = getIquanJNIEnv();
    LocalRef<jclass> clazz = env->GetObjectClass(self());
    if (!clazz.get()) {
        transJniException();
    }
    return clazz;
}

LocalRef<jstring> JObject::toString() {
    static auto method = javaClass()->getMethod<jstring()>("toString");
    return method(self());
}

std::string JObject::toStdString() {
    LocalRef<jstring> js = toString();
    return js->toStdString();
}

LocalRef<jstring> JString::fromStdString(const std::string &s) {
    auto env = getIquanJNIEnv();
    return {env->NewStringUTF(s.c_str())};
}

std::string JString::toStdString() {
    std::string s;
    JNIEnv *env = getIquanJNIEnv();
    const char *p = env->GetStringUTFChars(self(), nullptr);
    if (p) {
        s = std::string(p);
        env->ReleaseStringUTFChars(self(), p);
    }
    return s;

    return s;
}

LocalRef<jstring> JThrowable::getMessage() {
    static auto method = javaClass()->getMethod<jstring()>("getMessage");
    return method(self());
}

LocalRef<jthrowable> JThrowable::getCause() {
    static auto method = javaClass()->getMethod<jthrowable()>("getCause");
    return method(self());
}

std::string JObjectArray::javaDescriptor() { return "[" + JTypeTraits<jobject>::descriptor(); }

std::string JObjectArray::baseName() { return JObjectArray::javaDescriptor(); }

#define DEFINE_PRIMITIVE_ARRAY_METHODS(TYPE, NAME)                                                                     \
    template <>                                                                                                        \
    LocalRef<TYPE##Array> J##NAME##Array::newArray(size_t size) {                                                      \
        auto array = getIquanJNIEnv()->New##NAME##Array(size);                                                         \
        return {array};                                                                                                \
    }                                                                                                                  \
    template <>                                                                                                        \
    void J##NAME##Array::getRegion(jsize start, jsize length, TYPE *elements) {                                        \
        auto env = getIquanJNIEnv();                                                                                   \
        env->Get##NAME##ArrayRegion(self(), start, length, elements);                                                  \
        transJniException();                                                                                           \
    }                                                                                                                  \
    template <>                                                                                                        \
    std::vector<TYPE> J##NAME##Array::getRegion(jsize start, jsize length) {                                           \
        std::vector<TYPE> buf(length);                                                                                 \
        getRegion(start, length, buf.data());                                                                          \
        return buf;                                                                                                    \
    }                                                                                                                  \
    template <>                                                                                                        \
    void J##NAME##Array::setRegion(jsize start, jsize length, const TYPE *elements) {                                  \
        auto env = getIquanJNIEnv();                                                                                   \
        env->Set##NAME##ArrayRegion(self(), start, length, elements);                                                  \
    }

DEFINE_PRIMITIVE_ARRAY_METHODS(jboolean, Boolean)
DEFINE_PRIMITIVE_ARRAY_METHODS(jbyte, Byte)
DEFINE_PRIMITIVE_ARRAY_METHODS(jchar, Char)
DEFINE_PRIMITIVE_ARRAY_METHODS(jshort, Short)
DEFINE_PRIMITIVE_ARRAY_METHODS(jint, Int)
DEFINE_PRIMITIVE_ARRAY_METHODS(jlong, Long)
DEFINE_PRIMITIVE_ARRAY_METHODS(jfloat, Float)
DEFINE_PRIMITIVE_ARRAY_METHODS(jdouble, Double)

#undef DEFINE_PRIMITIVE_ARRAY_METHODS

std::string JByteArrayToStdString(AliasRef<JByteArray> byteArray) {
    std::string s;
    s.resize(byteArray->size());
    byteArray->getRegion(0, byteArray->size(), (jbyte *)s.data());
    return s;
}

LocalRef<JByteArray> JByteArrayFromStdString(const std::string &s) {
    LocalRef<JByteArray> ba = JByteArray::newArray(s.size());
    ba->setRegion(0, ba->size(), (const jbyte *)s.data());
    return ba;
}

LocalRef<JObjectArrayT<JStackTraceElement>> JThrowable::getStackTrace() {
    static auto method = javaClass()->getMethod<JObjectArrayT<JStackTraceElement>()>("getStackTrace");
    return method(self());
}

} // namespace iquan
