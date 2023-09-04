#include "iquan/jni/jnipp/jnipp.h"
#include "iquan/jni/test/testlib/IquanTestBase.h"

namespace iquan {

class JTmpObj1 : public JavaClass<JTmpObj1> {};
class JTmpObj2 : public JavaClass<JTmpObj2, JObject, jstring> {};
class JTmpObj3 {};
class TypeTraitsTest : public IquanTestBase {};

static_assert(std::is_same<JTmpObj1, CType<JTmpObj1>>::value, "");
static_assert(std::is_same<jobject, JType<JTmpObj1>>::value, "");
static_assert(std::is_same<JTmpObj2, CType<JTmpObj2>>::value, "");
static_assert(std::is_same<jstring, JType<JTmpObj2>>::value, "");

static_assert(IsJniPrimitive<jboolean>::value && IsJniPrimitive<jbyte>::value
                  && IsJniPrimitive<jchar>::value && IsJniPrimitive<jshort>::value
                  && IsJniPrimitive<jint>::value && IsJniPrimitive<jlong>::value
                  && IsJniPrimitive<jfloat>::value && IsJniPrimitive<jdouble>::value,
              "jni primitive check");

static_assert(!IsJniReference<jboolean>::value && !IsJniReference<jbyte>::value
                  && !IsJniReference<jchar>::value && !IsJniReference<jshort>::value
                  && !IsJniReference<jint>::value && !IsJniReference<jlong>::value
                  && !IsJniReference<jfloat>::value && !IsJniReference<jdouble>::value,
              "jni primitive is not reference");

static_assert(!IsJniWrapper<jboolean>::value && !IsJniWrapper<jbyte>::value
                  && !IsJniWrapper<jchar>::value && !IsJniWrapper<jshort>::value
                  && !IsJniWrapper<jint>::value && !IsJniWrapper<jlong>::value
                  && !IsJniWrapper<jfloat>::value && !IsJniWrapper<jdouble>::value,
              "jni primitive is not wrapper");

static_assert(IsJniReference<jobject>::value && IsJniReference<jclass>::value
                  && IsJniReference<jstring>::value && IsJniReference<jarray>::value
                  && IsJniReference<jobjectArray>::value && IsJniReference<jbooleanArray>::value
                  && IsJniReference<jbyteArray>::value && IsJniReference<jcharArray>::value
                  && IsJniReference<jshortArray>::value && IsJniReference<jintArray>::value
                  && IsJniReference<jlongArray>::value && IsJniReference<jfloatArray>::value
                  && IsJniReference<jdoubleArray>::value && IsJniReference<jthrowable>::value,
              "jni reference check");

static_assert(!IsJniReference<JObject>::value && !IsJniReference<JClass>::value
                  && !IsJniReference<JString>::value && !IsJniReference<JArray>::value
                  && !IsJniReference<JObjectArray>::value && !IsJniReference<JBooleanArray>::value
                  && !IsJniReference<JByteArray>::value && !IsJniReference<JCharArray>::value
                  && !IsJniReference<JShortArray>::value && !IsJniReference<JIntArray>::value
                  && !IsJniReference<JLongArray>::value && !IsJniReference<JFloatArray>::value
                  && !IsJniReference<JDoubleArray>::value && !IsJniReference<JThrowable>::value,
              "jni reference check");

static_assert(!IsJniWrapper<jobject>::value && !IsJniWrapper<jclass>::value
                  && !IsJniWrapper<jstring>::value && !IsJniWrapper<jarray>::value
                  && !IsJniWrapper<jobjectArray>::value && !IsJniWrapper<jbooleanArray>::value
                  && !IsJniWrapper<jbyteArray>::value && !IsJniWrapper<jcharArray>::value
                  && !IsJniWrapper<jshortArray>::value && !IsJniWrapper<jintArray>::value
                  && !IsJniWrapper<jlongArray>::value && !IsJniWrapper<jfloatArray>::value
                  && !IsJniWrapper<jdoubleArray>::value && !IsJniWrapper<jthrowable>::value,
              "jni wrapper check");

static_assert(IsJniWrapper<JObject>::value && IsJniWrapper<JClass>::value
                  && IsJniWrapper<JString>::value && IsJniWrapper<JArray>::value
                  && IsJniWrapper<JObjectArray>::value && IsJniWrapper<JBooleanArray>::value
                  && IsJniWrapper<JByteArray>::value && IsJniWrapper<JCharArray>::value
                  && IsJniWrapper<JShortArray>::value && IsJniWrapper<JIntArray>::value
                  && IsJniWrapper<JLongArray>::value && IsJniWrapper<JFloatArray>::value
                  && IsJniWrapper<JDoubleArray>::value && IsJniWrapper<JThrowable>::value,
              "jni wrapper check");

static_assert(
    IsJniReferenceOrWrapper<jobject>::value && IsJniReferenceOrWrapper<jclass>::value
        && IsJniReferenceOrWrapper<jstring>::value && IsJniReferenceOrWrapper<jarray>::value
        && IsJniReferenceOrWrapper<jobjectArray>::value
        && IsJniReferenceOrWrapper<jbooleanArray>::value
        && IsJniReferenceOrWrapper<jbyteArray>::value && IsJniReferenceOrWrapper<jcharArray>::value
        && IsJniReferenceOrWrapper<jshortArray>::value && IsJniReferenceOrWrapper<jintArray>::value
        && IsJniReferenceOrWrapper<jlongArray>::value && IsJniReferenceOrWrapper<jfloatArray>::value
        && IsJniReferenceOrWrapper<jdoubleArray>::value
        && IsJniReferenceOrWrapper<jthrowable>::value &&

        IsJniReferenceOrWrapper<JObject>::value && IsJniReferenceOrWrapper<JClass>::value
        && IsJniReferenceOrWrapper<JString>::value && IsJniReferenceOrWrapper<JArray>::value
        && IsJniReferenceOrWrapper<JObjectArray>::value
        && IsJniReferenceOrWrapper<JBooleanArray>::value
        && IsJniReferenceOrWrapper<JByteArray>::value && IsJniReferenceOrWrapper<JCharArray>::value
        && IsJniReferenceOrWrapper<JShortArray>::value && IsJniReferenceOrWrapper<JIntArray>::value
        && IsJniReferenceOrWrapper<JLongArray>::value && IsJniReferenceOrWrapper<JFloatArray>::value
        && IsJniReferenceOrWrapper<JDoubleArray>::value
        && IsJniReferenceOrWrapper<JThrowable>::value,

    "jni reference or wrapper check");

static_assert(!IsJniPrimitive<void>::value && !IsJniPrimitive<void *>::value
                  && !IsJniReference<void>::value && !IsJniReference<void *>::value
                  && !IsJniReferenceOrWrapper<void>::value
                  && !IsJniReferenceOrWrapper<void *>::value,
              "jni reference check");

TEST_F(TypeTraitsTest, testTypeTraits) {
#define CORE_CLASS_TYPES_CHECKS(ctype, jtype)                                                      \
    static_assert(std::is_same<ctype, CType<ctype>>::value, "ctype's ctype is itself");            \
    static_assert(std::is_same<ctype, CType<jtype>>::value, "jtype's ctype is ctype");             \
    static_assert(std::is_same<jtype, JType<jtype>>::value, "jtype's jtype is itself");            \
    static_assert(std::is_same<jtype, JType<ctype>>::value, "ctype's jtype is jtype");

    CORE_CLASS_TYPES_CHECKS(JObject, jobject)
    CORE_CLASS_TYPES_CHECKS(JClass, jclass)
    CORE_CLASS_TYPES_CHECKS(JString, jstring)
    CORE_CLASS_TYPES_CHECKS(JThrowable, jthrowable)

#undef CORE_CLASS_TYPES_CHECKS

    // should not compile
    // using CTmpObj3 = CType<JTmpObj3>;
    // using JTypeObj3T = JType<JTmpObj3>;

    static_assert(IsBaseJniReferenceOf<jobject, jobject>::value, "base jni reference check");
    static_assert(IsBaseJniReferenceOf<jobject, jclass>::value, "base jni reference check");
    static_assert(IsBaseJniReferenceOf<jobject, jstring>::value, "base jni reference check");
    static_assert(IsBaseJniReferenceOf<jobject, jarray>::value, "base jni reference check");
    static_assert(IsBaseJniReferenceOf<jobject, jobjectArray>::value, "base jni reference check");
    static_assert(IsBaseJniReferenceOf<jstring, jstring>::value, "base jni reference check");
    static_assert(IsBaseJniReferenceOf<jarray, jarray>::value, "base jni reference check");
    static_assert(IsBaseJniReferenceOf<jarray, jobjectArray>::value, "base jni reference check");
    static_assert(IsBaseJniReferenceOf<jarray, jbyteArray>::value, "base jni reference check");

    static_assert(IsBaseJniOf<jobject, jobject>::value, "base jni check");
    static_assert(IsBaseJniOf<jobject, JObject>::value, "base jni check");
    static_assert(IsBaseJniOf<JObject, jobject>::value, "base jni check");
    static_assert(IsBaseJniOf<JObject, JObject>::value, "base jni check");
    static_assert(IsBaseJniOf<jobject, jclass>::value, "base jni check");
    static_assert(IsBaseJniOf<JObject, JClass>::value, "base jni check");
    static_assert(IsBaseJniReferenceOf<JType<JObjectArrayT<JString>>, JType<JObjectArray>>::value,
                  "JObjectArrayT<JString> jtype is jobjectArray");
    static_assert(!IsBaseJniOf<JObjectArrayT<JString>, JObjectArray>::value,
                  "but JObjectArray not compatible with JObjectArrayT<JString>");
    ASSERT_EQ(1, 1);
}

} // namespace iquan
