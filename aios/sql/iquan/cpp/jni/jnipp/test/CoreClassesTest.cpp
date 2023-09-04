#include "iquan/jni/Jvm.h"
#include "iquan/jni/jnipp/jnipp.h"
#include "iquan/jni/test/testlib/IquanTestBase.h"

namespace iquan {

class JWrongString : public JavaClass<JWrongString, JObject, jstring> {
public:
    static constexpr const char *kJavaDescriptor = "Ljava/lang/StringNotExist;";
};

class CoreClassesTest : public IquanTestBase {};

TEST_F(CoreClassesTest, testFindClass) {
    std::cout << "findClassLocal[======================\n";
    {
        LocalRef<jclass> clazz = util::findClassLocal("java/lang/Object");
        std::cout << (void *)clazz.get() << std::endl;
    }
    std::cout << "findClassLocal]======================\n";

    std::cout << "findClassGlobal[======================\n";
    { GlobalRef<jclass> clazz = util::findClassGlobal("java/lang/Object"); }
    std::cout << "findClassGlobal]======================\n";
}

TEST_F(CoreClassesTest, testToStdString) {
    JNIEnv *env = Jvm::checkedEnv();
    const std::string s("hello world");
    jstring js = env->NewStringUTF(s.c_str());
    JObject o;
    o.set(js);
    ASSERT_EQ(s, o.toStdString());

    JString so;
    so.set(js);
    ASSERT_EQ(s, so.toStdString());
}

TEST_F(CoreClassesTest, testToStdString2) {
    JNIEnv *env = Jvm::checkedEnv();
    const std::string s("hello world");
    jstring js = env->NewStringUTF(s.c_str());
    JString o;
    o.set(js);
    ASSERT_EQ(s, o.toStdString());
}

TEST_F(CoreClassesTest, testClassNotExist) {
    try {
        JWrongString::javaClass();
        ASSERT_EQ(0, 1);
    } catch (JnippException &e) { std::cout << "expected ex: " << e.what() << std::endl; }
}

TEST_F(CoreClassesTest, testSelf) {
    //    ASSERT_EQ(val1, val2)
    JNIEnv *env = Jvm::checkedEnv();
    const std::string s("hello world");
    jstring js = env->NewStringUTF(s.c_str());
    JString o;
    o.set(js);
    ASSERT_EQ(js, o.self());
}

TEST_F(CoreClassesTest, testPrimitiveArray) {
    {
        LocalRef<jbooleanArray> ba = JBooleanArray::newArray(32);
        ASSERT_EQ(32, ba->size());
    }
    {
        LocalRef<jbyteArray> ba = JByteArray::newArray(32);
        ASSERT_EQ(32, ba->size());
        jbyte elements[32];
        for (int i = 0; i < 32; i++) {
            elements[i] = i;
        }
        ba->setRegion(0, 32, elements);
        jbyte elements2[32];
        ba->getRegion(0, 32, elements2);
        for (int i = 0; i < 32; i++) {
            ASSERT_EQ(elements[i], elements2[i]);
        }
        std::vector<jbyte> elements3 = ba->getRegion(0, 32);
        for (int i = 0; i < 32; i++) {
            ASSERT_EQ(elements[i], elements3[i]);
        }
    }
    {
        LocalRef<jcharArray> ba = JCharArray::newArray(32);
        ASSERT_EQ(32, ba->size());
    }
    {
        LocalRef<jshortArray> ba = JShortArray::newArray(32);
        ASSERT_EQ(32, ba->size());
    }
    {
        LocalRef<jintArray> ba = JIntArray::newArray(32);
        ASSERT_EQ(32, ba->size());

        jint elements[32];
        for (int i = 0; i < 32; i++) {
            elements[i] = i * 100;
        }
        ba->setRegion(0, 32, elements);
        jint elements2[32];
        ba->getRegion(0, 32, elements2);
        for (int i = 0; i < 32; i++) {
            ASSERT_EQ(elements[i], elements2[i]);
        }
        std::vector<jint> elements3 = ba->getRegion(0, 32);
        for (int i = 0; i < 32; i++) {
            ASSERT_EQ(elements[i], elements3[i]);
        }
    }
    {
        LocalRef<jlongArray> ba = JLongArray::newArray(32);
        ASSERT_EQ(32, ba->size());
    }
    {
        LocalRef<jfloatArray> ba = JFloatArray::newArray(32);
        ASSERT_EQ(32, ba->size());
    }
    {
        LocalRef<jdoubleArray> ba = JDoubleArray::newArray(32);
        ASSERT_EQ(32, ba->size());
    }
}

TEST_F(CoreClassesTest, testObjectArray) {
    ASSERT_TRUE((std::is_same<jobjectArray, typename JObjectArrayT<jstring>::jtype>::value));
    ASSERT_TRUE((std::is_same<jobjectArray, typename JObjectArrayT<JString>::jtype>::value));
    ASSERT_TRUE((std::is_same<jobjectArray, typename JObjectArrayT<jobjectArray>::jtype>::value));
    ASSERT_TRUE((std::is_same<jobjectArray, typename JObjectArrayT<JObjectArray>::jtype>::value));
    ASSERT_TRUE(
        (std::is_same<jobjectArray, typename JObjectArrayT<JObjectArrayT<jstring>>::jtype>::value));

    ASSERT_TRUE((std::is_same<jobjectArray, JType<JObjectArrayT<jstring>>>::value));
    ASSERT_TRUE((std::is_same<jobjectArray, JType<JObjectArrayT<JString>>>::value));
    ASSERT_TRUE((std::is_same<jobjectArray, JType<JObjectArrayT<jobjectArray>>>::value));
    ASSERT_TRUE((std::is_same<jobjectArray, JType<JObjectArrayT<JObjectArray>>>::value));
    ASSERT_TRUE((std::is_same<jobjectArray, JType<JObjectArrayT<JObjectArrayT<jstring>>>>::value));

    ASSERT_TRUE((std::is_same<jstring, typename JObjectArrayT<jstring>::entry_jtype>::value));
    ASSERT_TRUE((std::is_same<jstring, typename JObjectArrayT<JString>::entry_jtype>::value));
    ASSERT_TRUE(
        (std::is_same<jobjectArray, typename JObjectArrayT<jobjectArray>::entry_jtype>::value));
    ASSERT_TRUE(
        (std::is_same<jobjectArray, typename JObjectArrayT<JObjectArray>::entry_jtype>::value));
    ASSERT_TRUE((std::is_same<jobjectArray,
                              typename JObjectArrayT<JObjectArrayT<jstring>>::entry_jtype>::value));

    ASSERT_EQ("[Ljava/lang/String;", JTypeTraits<JObjectArrayT<jstring>>::descriptor());
    ASSERT_EQ("[Ljava/lang/String;", JTypeTraits<JObjectArrayT<JString>>::descriptor());
    ASSERT_EQ("[[Ljava/lang/Object;", JTypeTraits<JObjectArrayT<jobjectArray>>::descriptor());
    ASSERT_EQ("[[Ljava/lang/Object;", JTypeTraits<JObjectArrayT<JObjectArray>>::descriptor());
    ASSERT_EQ("[[Ljava/lang/String;",
              JTypeTraits<JObjectArrayT<JObjectArrayT<jstring>>>::descriptor());
    ASSERT_EQ("[[Ljava/lang/String;",
              JTypeTraits<JObjectArrayT<JObjectArrayT<JString>>>::descriptor());

    {
        LocalRef<JObjectArrayT<jstring>> sa = JObjectArrayT<jstring>::newArray(32);
        ASSERT_EQ(32, sa->size());
        for (int i = 0; i < 32; i++) {
            ASSERT_EQ(nullptr, sa->getElement(i).get());
            LocalRef<jstring> s = JString::fromStdString("s" + std::to_string(i));
            sa->setElement(i, s);
        }
        for (int i = 0; i < 32; i++) {
            LocalRef<jstring> js = sa->getElement(i);
            std::string s = "s" + std::to_string(i);
            ASSERT_EQ(s, js->toStdString());
        }
    }

    {
        auto aa = JObjectArrayT<JObjectArrayT<jstring>>::newArray(32);
        ASSERT_EQ(32, aa->size());
        for (int i = 0; i < 32; i++) {
            ASSERT_EQ(nullptr, aa->getElement(i).get());
            auto sa = JObjectArrayT<jstring>::newArray(32);
            ASSERT_EQ(32, sa->size());
            for (int j = 0; j < 32; j++) {
                ASSERT_EQ(nullptr, sa->getElement(j).get());
                LocalRef<jstring> s = JString::fromStdString("s" + std::to_string(j));
                sa->setElement(j, s);
            }
            aa->setElement(i, sa);
        }
        for (int i = 0; i < 32; i++) {
            auto sa = aa->getElement(i);
            ASSERT_EQ(32, sa->size());
            for (int j = 0; j < 32; j++) {
                auto js = sa->getElement(j);
                std::string s = "s" + std::to_string(j);
                ASSERT_EQ(s, js->toStdString());
            }
        }
    }
}

TEST_F(CoreClassesTest, testNewObject) {
    auto jcls = JString::javaClass();
    auto constructor = jcls->getConstructor<jstring()>();
    LocalRef<jstring> s = jcls->newObject(constructor);
    ASSERT_EQ("", s->toStdString());

    std::string s1("hello world");
    auto constructor1 = jcls->getConstructor<jstring(jstring)>();
    auto env = Jvm::env();
    LocalRef<jstring> js1 {env->NewStringUTF(s1.c_str())};
    LocalRef<jstring> s2 = jcls->newObject(constructor1, js1.get());
    ASSERT_EQ(s1, s2->toStdString());
    std::cout << "s2: " << s2->toStdString() << std::endl;

    try {
        auto constructor2 = jcls->getConstructor<jstring(jint)>();
        (void)constructor2;
        ASSERT_EQ(0, 1);
    } catch (const JnippException &e) { std::cout << "expected ex: " << e.what() << std::endl; }
}

TEST_F(CoreClassesTest, testNewInstance) {
    LocalRef<JString> s = JString::newInstance();
    ASSERT_EQ("", s->toStdString());

    auto method_isEmpty = JString::javaClass()->getMethod<jboolean()>("isEmpty");
    jboolean isEmpty = method_isEmpty(s.get());
    ASSERT_EQ(JNI_TRUE, isEmpty);

    std::string s1("hello world");
    LocalRef<jstring> js1 = JString::fromStdString(s1);
    LocalRef<JString> s2 = JString::newInstance(js1.get());
    ASSERT_EQ(s1, s2->toStdString());
    std::cout << "s2: " << s2->toStdString() << std::endl;

    auto method_length = JString::javaClass()->getMethod<jint()>("length");
    jint s2Length = method_length(s2.get());
    ASSERT_EQ(s1.size(), s2Length);
    isEmpty = method_isEmpty(s2.get());
    ASSERT_EQ(JNI_FALSE, isEmpty);

    try {
        // constructor throws
        JString::newInstance(100);
        ASSERT_EQ(0, 1);
    } catch (const JnippException &e) { std::cout << "expected ex: " << e.what() << std::endl; }

    LocalRef<jbyteArray> ba = JByteArray::newArray(s1.size());
    ba->setRegion(0, s1.size(), (const jbyte *)s1.c_str());
    ASSERT_EQ(s1.size(), ba->size());
    LocalRef<jstring> charsetUtf8 = JString::fromStdString("UTF-8");
    LocalRef<jstring> s3 = JString::newInstance(ba.get(), charsetUtf8.get());
    std::cout << "s3: " << s3->toStdString() << std::endl;
    auto method_getBytes = JString::javaClass()->getMethod<jbyteArray(jstring)>("getBytes");
    LocalRef<jbyteArray> ba1 = method_getBytes(s3.get(), charsetUtf8.get());
    std::vector<jbyte> ba1bytes = ba1->getRegion(0, ba1->size());
    std::string s3bytes((char *)ba1bytes.data(), ba1bytes.size());
    std::cout << "s3bytes:  " << s3bytes << std::endl;
    ASSERT_EQ(s1, s3bytes);

    LocalRef<jstring> charsetUnknown = JString::fromStdString("ukkk");

    try {
        // method call throws
        method_getBytes(s3.get(), charsetUnknown.get());
        ASSERT_EQ(0, 1);
    } catch (const JnippException &e) { std::cout << "expected ex: " << e.what() << std::endl; }

    try {
        // constructor throws
        LocalRef<jstring> s4 = JString::newInstance(ba.get(), charsetUnknown.get());
        ASSERT_EQ(0, 1);
    } catch (const JnippException &e) { std::cout << "expected ex: " << e.what() << std::endl; }
}

TEST_F(CoreClassesTest, testThrowable) {
    std::string msg("shenme");
    LocalRef<jstring> jmsg = JString::fromStdString(msg);
    LocalRef<jthrowable> t = JThrowable::newInstance(jmsg);
    std::string tmsg = t->getMessage()->toStdString();
    ASSERT_EQ(msg, tmsg);
    ASSERT_EQ(nullptr, t->getCause().get());

    LocalRef<JStackTraceElement> stackTraceElement0
        = JStackTraceElement::newInstance("class0", "method0", "file0", 0);
    LocalRef<JStackTraceElement> stackTraceElement1
        = JStackTraceElement::newInstance("class1", "method1", "file1", 0);
    LocalRef<JObjectArrayT<JStackTraceElement>> stackTraces
        = JObjectArrayT<JStackTraceElement>::newArray(2);
    ASSERT_EQ(2, stackTraces->size());
    stackTraces->setElement(0, stackTraceElement0);
    stackTraces->setElement(1, stackTraceElement1);

    auto method_setStackTrace
        = JThrowable::javaClass()->getMethod<void(JObjectArrayT<JStackTraceElement>)>(
            "setStackTrace");
    method_setStackTrace(t.get(), stackTraces);
    auto stackTraces1 = t->getStackTrace();
    ASSERT_EQ(2, stackTraces1->size());
    ASSERT_TRUE(util::isSameObject(stackTraces->getElement(0), stackTraces1->getElement(0)));
    ASSERT_TRUE(util::isSameObject(stackTraces->getElement(1), stackTraces1->getElement(1)));
}

} // namespace iquan
