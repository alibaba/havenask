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

#include "Meta-fwd.h"
#include "References-fwd.h"
#include "TypeTraits.h"
#include "jni.h"

namespace iquan {

class JMethodBase {
public:
    explicit operator bool() const noexcept {
        return _methodId != nullptr;
    }
    jmethodID getId() const noexcept {
        return _methodId;
    }

protected:
    explicit JMethodBase(jmethodID methodId = nullptr) noexcept
        : _methodId(methodId) {}

private:
    jmethodID _methodId;
};

#define DEFINE_PRIMITIVE_METHOD_CLASS(TYPE)                                                        \
    template <typename... Params>                                                                  \
    class JMethod<TYPE(Params...)> : public JMethodBase {                                          \
    public:                                                                                        \
        using JMethodBase::JMethodBase;                                                            \
        JMethod() noexcept {}                                                                      \
        template <typename... Args>                                                                \
        TYPE operator()(AliasRef<jobject> self, Args &&...args);                                   \
        friend class JClass;                                                                       \
    };

DEFINE_PRIMITIVE_METHOD_CLASS(void)
DEFINE_PRIMITIVE_METHOD_CLASS(jboolean)
DEFINE_PRIMITIVE_METHOD_CLASS(jbyte)
DEFINE_PRIMITIVE_METHOD_CLASS(jchar)
DEFINE_PRIMITIVE_METHOD_CLASS(jshort)
DEFINE_PRIMITIVE_METHOD_CLASS(jint)
DEFINE_PRIMITIVE_METHOD_CLASS(jlong)
DEFINE_PRIMITIVE_METHOD_CLASS(jfloat)
DEFINE_PRIMITIVE_METHOD_CLASS(jdouble)

DEFINE_PRIMITIVE_METHOD_CLASS(bool)
DEFINE_PRIMITIVE_METHOD_CLASS(std::string)
#undef DEFINE_PRIMITIVE_METHOD_CALSS

template <typename R, typename... Params>
class JMethod<R(Params...)> : public JMethodBase {
public:
    using cret = CType<R>;
    using jret = JType<R>;
    using JMethodBase::JMethodBase;
    JMethod() noexcept {}
    template <typename... Args>
    LocalRef<cret> operator()(AliasRef<jobject> self, Args &&...args);

    friend class JClass;
};

template <typename F>
class JConstructor : private JMethod<F> {
public:
    using JMethod<F>::JMethod;

private:
    JConstructor(const JMethod<F> &other)
        : JMethod<F>(other.getId()) {}
    friend class JClass;
};

#define DEFINE_PRIMITIVE_STATIC_METHOD_CALSS(TYPE)                                                 \
    template <typename... Params>                                                                  \
    class JStaticMethod<TYPE(Params...)> : public JMethodBase {                                    \
    public:                                                                                        \
        using JMethodBase::JMethodBase;                                                            \
        JStaticMethod() noexcept = default;                                                        \
        JStaticMethod(const JStaticMethod &other) noexcept = default;                              \
        JStaticMethod(JStaticMethod &&other) noexcept = default;                                   \
        template <typename... Args>                                                                \
        TYPE operator()(AliasRef<jclass>, Args &&...args);                                         \
        friend class JClass;                                                                       \
    };

DEFINE_PRIMITIVE_STATIC_METHOD_CALSS(void)
DEFINE_PRIMITIVE_STATIC_METHOD_CALSS(jboolean)
DEFINE_PRIMITIVE_STATIC_METHOD_CALSS(jbyte)
DEFINE_PRIMITIVE_STATIC_METHOD_CALSS(jchar)
DEFINE_PRIMITIVE_STATIC_METHOD_CALSS(jshort)
DEFINE_PRIMITIVE_STATIC_METHOD_CALSS(jint)
DEFINE_PRIMITIVE_STATIC_METHOD_CALSS(jlong)
DEFINE_PRIMITIVE_STATIC_METHOD_CALSS(jfloat)
DEFINE_PRIMITIVE_STATIC_METHOD_CALSS(jdouble)

DEFINE_PRIMITIVE_STATIC_METHOD_CALSS(bool)
DEFINE_PRIMITIVE_STATIC_METHOD_CALSS(std::string)
#undef DEFINE_PRIMITIVE_STATIC_METHOD_CALSS

template <typename R, typename... Params>
class JStaticMethod<R(Params...)> : public JMethodBase {
public:
    using cret = CType<R>;
    using jret = JType<R>;
    using JMethodBase::JMethodBase;
    JStaticMethod() noexcept = default;
    JStaticMethod(const JStaticMethod &other) noexcept = default;
    template <typename... Args>
    LocalRef<cret> operator()(AliasRef<jclass> cls, Args &&...args);
};

#define DEFINE_PRIMITIVE_AND_ARRAY_TRAITS(TYPE, DSC)                                               \
    template <>                                                                                    \
    struct JTypeTraits<TYPE> {                                                                     \
        static std::string descriptor() {                                                          \
            return std::string {#DSC};                                                             \
        }                                                                                          \
        static std::string baseName() {                                                            \
            return descriptor();                                                                   \
        }                                                                                          \
    };                                                                                             \
    template <>                                                                                    \
    struct JTypeTraits<TYPE##Array> {                                                              \
        static std::string descriptor() {                                                          \
            return std::string {"[" #DSC};                                                         \
        }                                                                                          \
        static std::string baseName() {                                                            \
            return descriptor();                                                                   \
        }                                                                                          \
        using entry_type = TYPE;                                                                   \
    };

DEFINE_PRIMITIVE_AND_ARRAY_TRAITS(jboolean, Z)
DEFINE_PRIMITIVE_AND_ARRAY_TRAITS(jbyte, B)
DEFINE_PRIMITIVE_AND_ARRAY_TRAITS(jchar, C)
DEFINE_PRIMITIVE_AND_ARRAY_TRAITS(jshort, S)
DEFINE_PRIMITIVE_AND_ARRAY_TRAITS(jint, I)
DEFINE_PRIMITIVE_AND_ARRAY_TRAITS(jlong, J)
DEFINE_PRIMITIVE_AND_ARRAY_TRAITS(jfloat, F)
DEFINE_PRIMITIVE_AND_ARRAY_TRAITS(jdouble, D)

#undef DEFINE_PRIMITIVE_AND_ARRAY_TRAITS

template <>
struct JTypeTraits<void> {
    static std::string descriptor() {
        return std::string("V");
    }
};

template <typename T>
struct JTypeTraits {
private:
    using ctype = CType<T>;

public:
    // used in signature
    static std::string descriptor() {
        if (ctype::kJavaDescriptor) {
            return ctype::kJavaDescriptor;
        }
        return ctype::javaDescriptor();
    }
    // used in findClass
    static std::string baseName() {
        if (ctype::kJavaDescriptor) {
            std::string baseName = ctype::kJavaDescriptor;
            return baseName.substr(1, baseName.size() - 2);
        }
        return ctype::baseName();
    }
};

template <typename R, typename... Args>
struct JMethodTraits<R(Args...)> {
    static std::string descriptor();
    static std::string constructor_descriptor();
};

template <typename T>
struct JMethodTraitsFromCxx;

} // namespace iquan

#include "Meta-inl.h"
