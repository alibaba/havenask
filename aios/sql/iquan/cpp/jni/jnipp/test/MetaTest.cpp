#include "iquan/jni/jnipp/jnipp.h"
#include "iquan/jni/test/testlib/IquanTestBase.h"

namespace iquan {

class MetaTest : public IquanTestBase {};

TEST_F(MetaTest, testMethodDescriptor) {
    ASSERT_EQ("()Ljava/lang/String;", JMethodTraits<jstring()>::descriptor());
    ASSERT_EQ("()Ljava/lang/String;", JMethodTraitsFromCxx<jstring()>::descriptor());
    ASSERT_EQ("()Ljava/lang/String;", JMethodTraitsFromCxx<JString()>::descriptor());
    ASSERT_EQ("(Ljava/lang/String;)Ljava/lang/String;",
              JMethodTraitsFromCxx<jstring(jstring)>::descriptor());
    ASSERT_EQ("(Ljava/lang/String;)Ljava/lang/String;",
              JMethodTraitsFromCxx<JString(JString)>::descriptor());
    ASSERT_EQ("(Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;",
              JMethodTraitsFromCxx<jstring(jstring, jstring)>::descriptor());
    ASSERT_EQ("(ZBCS)Ljava/lang/String;",
              JMethodTraits<jstring(jboolean, jbyte, jchar, jshort)>::descriptor());
    ASSERT_EQ("(IJLjava/lang/String;F)D",
              JMethodTraits<jdouble(jint, jlong, jstring, jfloat)>::descriptor());

    ASSERT_EQ("(Ljava/lang/String;)[B", JMethodTraits<jbyteArray(jstring)>::descriptor());

    ASSERT_EQ("(Ljava/lang/String;)[B", JMethodTraitsFromCxx<JByteArray(jstring)>::descriptor());

    ASSERT_EQ("([ZLjava/lang/String;[S)[Z",
              JMethodTraits<jbooleanArray(jbooleanArray, jstring, jshortArray)>::descriptor());

    ASSERT_EQ("(I[B)V", JMethodTraits<jbyteArray(int, jbyteArray)>::constructor_descriptor());
}

} // namespace iquan
