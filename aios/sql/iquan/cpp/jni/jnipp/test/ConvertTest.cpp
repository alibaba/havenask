#include "iquan/jni/jnipp/jnipp.h"
#include "iquan/jni/test/testlib/IquanTestBase.h"

namespace iquan {

void testPassThruFunc1(...) {
    std::cout << "in testPassThruFunc" << std::endl;
}

template <typename... Args>
void testPassThruFunc2(Args... args) {
    testPassThruFunc1(impl::callToJni(Convert<typename std::decay<Args>::type>::toCall(args))...);
}

namespace temp {
class JInteger : public JavaClass<JInteger> {
public:
    static constexpr const char *kJavaDescriptor = "Ljava/lang/Integer;";
};
} // namespace temp

class ConvertTest : public IquanTestBase {};

TEST_F(ConvertTest, testArgsConvertSimple) {
    std::string s1 = "hello";
    LocalRef<jstring> js1 = JString::fromStdString(s1);
    std::string s2 = "world";
    LocalRef<jstring> js2 = JString::fromStdString(s2);
    std::string s3 = "helloworld";
    {
        // the jstring created from s2 should be released after method call
        std::cout << "=================one method call [=================\n";
        testPassThruFunc2(s2);
        std::cout << "=================one method call ]=================\n";
    }
    {
        auto method = JString::javaClass()->getMethod<jboolean()>("isEmpty");
        auto isEmpty = method(js1.get());
        ASSERT_FALSE((bool)isEmpty);

        auto bmethod = JString::javaClass()->getMethod<bool()>("isEmpty");
        bool bisEmpty = bmethod(js1.get());
        ASSERT_FALSE(bisEmpty);

        auto toStringMethod = JString::javaClass()->getMethod<std::string()>("toString");
        std::string js1ToString = toStringMethod(js1.get());
        ASSERT_EQ(s1, js1ToString);
    }
    {
        auto method = JString::javaClass()->getMethod<jstring(jstring)>("concat");
        std::cout << "=================one method call [=================\n";
        LocalRef<jstring> js3 = method(js1.get(), js2.get());
        std::cout << "=================one method call ]=================\n";
        ASSERT_EQ(s3, js3->toStdString());
        LocalRef<jstring> js4 = method(js1.get(), js1.get());
        ASSERT_EQ((s1 + s1), js4->toStdString());
        auto method_indexOf = JString::javaClass()->getMethod<jint(jstring, jint)>("indexOf");
        jint index = method_indexOf(js4.get(), js1.get(), 0);
        ASSERT_EQ(0, index);
        index = method_indexOf(js4.get(), js1.get(), 1);
        ASSERT_EQ(5, index);
    }
    {
        auto method = JString::javaClass()->getMethod<jstring(std::string)>("concat");
        std::cout << "=================one method call [=================\n";
        LocalRef<jstring> js3 = method(js1.get(), s2);
        std::cout << "=================one method call ]=================\n";
        ASSERT_EQ(s3, js3->toStdString());
        LocalRef<jstring> js4 = method(js1.get(), s1);
        ASSERT_EQ((s1 + s1), js4->toStdString());
        auto method_indexOf = JString::javaClass()->getMethod<jint(std::string, jint)>("indexOf");
        jint index = method_indexOf(js4.get(), s1, 0);
        ASSERT_EQ(0, index);
        index = method_indexOf(js4.get(), s1, 1);
        ASSERT_EQ(5, index);
    }
    {
        auto method = JString::javaClass()->getMethod<jstring(const char *)>("concat");
        std::cout << "=================one method call [=================\n";
        LocalRef<jstring> js3 = method(js1.get(), s2.c_str());
        std::cout << "=================one method call ]=================\n";
        ASSERT_EQ(s3, js3->toStdString());
    }
    {
        auto method = temp::JInteger::javaClass()->getStaticMethod<std::string(int)>("toString");
        int t = 101;
        std::string s = method(temp::JInteger::javaClass().get(), t);
        ASSERT_EQ(std::to_string(t), s);

        int t1 = 23;
        auto method_sum = temp::JInteger::javaClass()->getStaticMethod<int(int, int)>("sum");
        int sum = method_sum(temp::JInteger::javaClass().get(), t, t1);
        ASSERT_EQ((t + t1), sum);

        auto method_intValue = temp::JInteger::javaClass()->getMethod<jint()>("intValue");

        auto method_valueOf_i
            = temp::JInteger::javaClass()->getStaticMethod<temp::JInteger(jint)>("valueOf");
        LocalRef<temp::JInteger> jt2 = method_valueOf_i(temp::JInteger::javaClass().get(), t1);
        jint t2 = method_intValue(jt2.get());
        ASSERT_EQ(t1, t2);

        auto method_valueOf_s
            = temp::JInteger::javaClass()->getStaticMethod<temp::JInteger(jstring)>("valueOf");
        LocalRef<jstring> t1js = JString::fromStdString(std::to_string(t1));
        LocalRef<temp::JInteger> jt3
            = method_valueOf_s(temp::JInteger::javaClass().get(), t1js.get());
        jint t3 = method_intValue(jt3.get());
        ASSERT_EQ(t1, t3);

        auto method_valueOf_stdString
            = temp::JInteger::javaClass()->getStaticMethod<temp::JInteger(std::string)>("valueOf");
        LocalRef<temp::JInteger> jt4
            = method_valueOf_stdString(temp::JInteger::javaClass().get(), std::to_string(t1));
        jint t4 = method_intValue(jt4.get());
        ASSERT_EQ(t1, t4);
    }
    {
        auto method = JString::javaClass()->getMethod<std::string(std::string)>("concat");
        std::cout << "=================one method call [=================\n";
        std::string js3 = method(js1.get(), s2);
        std::cout << "=================one method call ]=================\n";
        ASSERT_EQ(s3, js3);
    }
    {
        auto method = JString::javaClass()->getMethod<std::string(jstring)>("concat");
        std::cout << "=================one method call [=================\n";
        std::string js3 = method(js1.get(), JString::fromStdString(s2));
        std::cout << "=================one method call ]=================\n";
        ASSERT_EQ(s3, js3);
    }
    {
        auto method = JString::javaClass()->getMethod<jstring(std::string)>("concat");
        std::cout << "=================one method call [=================\n";
        LocalRef<jstring> js3 = method(js1.get(), s2);
        std::cout << "=================one method call ]=================\n";
        ASSERT_EQ(s3, js3->toStdString());
    }
    {
        auto method = JString::javaClass()->getMethod<jstring(jstring)>("concat");
        std::cout << "=================one method call [=================\n";
        LocalRef<jstring> js3 = method(js1.get(), JString::fromStdString(s2));
        std::cout << "=================one method call ]=================\n";
        ASSERT_EQ(s3, js3->toStdString());
    }
    ASSERT_EQ(1, 1);
}

TEST_F(ConvertTest, testConstructorArgsConvertSimple) {
    int i = 100;
    LocalRef<temp::JInteger> ji = temp::JInteger::newInstance(std::to_string(i));
    auto method_intValue = temp::JInteger::javaClass()->getMethod<jint()>("intValue");
    jint iv = method_intValue(ji.get());
    ASSERT_EQ(i, iv);

    LocalRef<temp::JInteger> ji1 = temp::JInteger::newInstance(std::to_string(i).c_str());
    jint iv1 = method_intValue(ji.get());
    ASSERT_EQ(i, iv1);
}

} // namespace iquan
