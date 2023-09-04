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
#include <vector>

#include "iquan/jni/jnipp/CoreClasses-fwd.h"
#include "iquan/jni/jnipp/Meta-fwd.h"
#include "iquan/jni/jnipp/References.h"

namespace iquan {

namespace impl {
template <typename JC, typename... Args>
static LocalRef<JC> newInstance(Args &&...args);
template <typename JC, typename C, typename... Args>
static LocalRef<JC> newInstanceWithClassLoader(C clazz, Args &&...args);
} // namespace impl

namespace util {
LocalRef<jclass> findClassLocal(const std::string &name);
GlobalRef<jclass> findClassGlobal(const std::string &name);
jmethodID getMethodID(jclass clazz, const std::string &name, const std::string &signature);
jmethodID getStaticMethodID(jclass clazz, const std::string &name, const std::string &signature);
bool isSameObject(AliasRef<jobject> lhs, AliasRef<jobject> rhs);
} // namespace util

#define SEAL_IN_REF(T)                                                                             \
    T() noexcept = default;                                                                        \
    T(const T &other) = default;                                                                   \
    T(T &&other) = default;                                                                        \
    template <typename U, typename Alloc>                                                          \
    friend class StrongRef;                                                                        \
    template <typename U>                                                                          \
    friend class AliasRef;

class JObject {
public:
    static constexpr const char *kJavaDescriptor = "Ljava/lang/Object;";
    static constexpr const char *javaDescriptor() {
        return nullptr;
    }
    static constexpr const char *baseName() {
        return nullptr;
    }
    using jtype = jobject;
    jobject get() const noexcept {
        return _ref;
    }
    void set(jobject ref) noexcept {
        _ref = ref;
    }
    jobject self() const noexcept {
        return get();
    }
    LocalRef<jclass> getClass() const;
    static AliasRef<jclass> javaClass();
    LocalRef<jstring> toString();
    std::string toStdString();

protected:
    // SEAL_IN_REF(JObject)
private:
    jobject _ref {nullptr};
};

template <typename T, typename Base = JObject, typename JniType = jobject>
class JavaClass : public Base {
public:
    static_assert(std::is_base_of<JObject, Base>::value, "Base root must be JObject");
    using jtype = JniType;
    jtype get() const noexcept {
        return static_cast<jtype>(JObject::get());
    }
    void set(jtype o) noexcept {
        JObject::set(o);
    }
    jtype self() const noexcept {
        return get();
    }
    static AliasRef<jclass> javaClass();

    template <typename... Args>
    static LocalRef<T> newInstance(Args &&...args) {
        return impl::newInstance<T>(args...);
    }

    template <typename C, typename... Args>
    static LocalRef<T> newInstanceWithClassLoader(C clazz, Args &&...args) {
        return impl::newInstanceWithClassLoader<T>(clazz, args...);
    }

protected:
    // SEAL_IN_REF(JavaClass)
};

class JClass : public JavaClass<JClass, JObject, jclass> {
public:
    static constexpr const char *kJavaDescriptor = "Ljava/lang/Class;";
    template <typename F>
    JConstructor<F> getConstructor() const;
    template <typename F>
    JConstructor<F> getConstructor(const std::string &signature) const;
    template <typename F>
    JMethod<F> getMethod(const std::string &name) const;
    template <typename F>
    JMethod<F> getMethod(const std::string &name, const std::string &signature) const;
    template <typename F>
    JMethod<F> getStaticMethod(const std::string &name) const;
    template <typename F>
    JMethod<F> getStaticMethod(const std::string &name, const std::string &signature) const;
    template <typename R, typename... Params, typename... Args>
    LocalRef<R> newObject(JConstructor<R(Params...)> constructor, Args &&...args) const;

protected:
    // SEAL_IN_REF(JClass)
};

class JString : public JavaClass<JString, JObject, jstring> {
public:
    static constexpr const char *kJavaDescriptor = "Ljava/lang/String;";
    static LocalRef<jstring> fromStdString(const std::string &s);
    std::string toStdString();

protected:
    // SEAL_IN_REF(JString)
};

class JArray : public JavaClass<JArray, JObject, jarray> {
public:
    static constexpr const char *kJavaDescriptor = nullptr;
    size_t size() const noexcept;

protected:
    // SEAL_IN_REF(JArray)
};

class JObjectArray : public JavaClass<JObjectArray, JArray, jobjectArray> {
public:
    using entry_jtype = jobject;
    static constexpr const char *kJavaDescriptor = nullptr;
    static std::string javaDescriptor();
    static std::string baseName();

protected:
    // SEAL_IN_REF(JObjectArray)
};

template <typename T>
class JObjectArrayT : public JavaClass<JObjectArrayT<T>, JObjectArray, jobjectArray> {
public:
    using Base = JavaClass<JObjectArrayT<T>, JObjectArray, jobjectArray>;
    using entry_jtype = JType<T>;
    static constexpr const char *kJavaDescriptor = nullptr;
    static std::string javaDescriptor();
    static std::string baseName();
    using Base::get;
    using Base::self;
    using Base::set;
    static LocalRef<JObjectArrayT<T>> newArray(size_t count);
    void setElement(size_t idx, AliasRef<T> value);
    LocalRef<T> getElement(size_t idx);

protected:
    // SEAL_IN_REF(JObjectArrayT)
};

template <typename jtypeArray>
class JPrimitiveArray : public JavaClass<JPrimitiveArray<jtypeArray>, JArray, jtypeArray> {
public:
    using T = typename JTypeTraits<jtypeArray>::entry_type;
    static constexpr const char *kJavaDescriptor = nullptr;
    static std::string javaDescriptor();
    static std::string baseName();
    static LocalRef<jtypeArray> newArray(size_t count);
    void getRegion(jsize start, jsize length, T *buf);
    std::vector<T> getRegion(jsize start, jsize length);
    void setRegion(jsize start, jsize length, const T *buf);

protected:
    // SEAL_IN_REF(JPrimitiveArray)
};

#define DEFINE_PRIMITIVE_ARRAY_TYPE_MAP(ctype, jtype)                                              \
    using ctype = JPrimitiveArray<jtype>;                                                          \
    template <>                                                                                    \
    struct CTypeT<jtype> {                                                                         \
        using type = ctype;                                                                        \
    };

DEFINE_PRIMITIVE_ARRAY_TYPE_MAP(JBooleanArray, jbooleanArray)
DEFINE_PRIMITIVE_ARRAY_TYPE_MAP(JByteArray, jbyteArray)
DEFINE_PRIMITIVE_ARRAY_TYPE_MAP(JCharArray, jcharArray)
DEFINE_PRIMITIVE_ARRAY_TYPE_MAP(JShortArray, jshortArray)
DEFINE_PRIMITIVE_ARRAY_TYPE_MAP(JIntArray, jintArray)
DEFINE_PRIMITIVE_ARRAY_TYPE_MAP(JLongArray, jlongArray)
DEFINE_PRIMITIVE_ARRAY_TYPE_MAP(JFloatArray, jfloatArray)
DEFINE_PRIMITIVE_ARRAY_TYPE_MAP(JDoubleArray, jdoubleArray)

#undef DEFINE_PRIMITIVE_ARRAY_TYPE_MAP

std::string JByteArrayToStdString(AliasRef<JByteArray> byteArray);
LocalRef<JByteArray> JByteArrayFromStdString(const std::string &s);

class JStackTraceElement : public JavaClass<JStackTraceElement> {
public:
    static constexpr const char *kJavaDescriptor = "Ljava/lang/StackTraceElement;";
};

class JThrowable : public JavaClass<JThrowable, JObject, jthrowable> {
public:
    static constexpr const char *kJavaDescriptor = "Ljava/lang/Throwable;";
    LocalRef<jstring> getMessage();
    LocalRef<jthrowable> getCause();
    LocalRef<JObjectArrayT<JStackTraceElement>> getStackTrace();

protected:
    // SEAL_IN_REF(JThrowable)
};

} // namespace iquan

#include "CoreClasses-inl.h"
