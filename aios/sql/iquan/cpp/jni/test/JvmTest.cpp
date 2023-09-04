#include "iquan/jni/Jvm.h"

#include <thread>

#include "autil/TimeUtility.h"
#include "iquan/jni/test/testlib/IquanTestBase.h"

namespace iquan {

class JvmTest : public IquanTestBase {};

TEST_F(JvmTest, testGetEnv) {
    JNIEnv *env = Jvm::env();
    ASSERT_NE(env, nullptr);

    JNIEnv *env1 = Jvm::env();
    ASSERT_EQ(env1, env);
}

TEST_F(JvmTest, testMTGetEnv) {
    std::thread t([]() {
        JNIEnv *env = Jvm::env();
        ASSERT_NE(env, nullptr);

        JNIEnv *env1 = Jvm::env();
        ASSERT_EQ(env1, env);
    });
    t.join();
}

TEST_F(JvmTest, testGetEnvPerf) {
    constexpr size_t times = 1000 * 1000;
    int64_t beginTime = 0;

    {
        beginTime = autil::TimeUtility::currentTime();
        for (size_t i = 0; i < times; i++) {
            __asm__ __volatile__("" ::: "memory");
        }
        AUTIL_LOG(INFO, "do nothing elapsed: %ld", autil::TimeUtility::currentTime() - beginTime);
    }

    JNIEnv *env = Jvm::env();
    std::string s("hello world");
    jstring js = env->NewStringUTF(s.c_str());

    {
        beginTime = autil::TimeUtility::currentTime();
        for (size_t i = 0; i < times; i++) {
            jobject ts = env->NewLocalRef(js);
            __asm__ __volatile__("" ::: "memory");
            env->DeleteLocalRef(ts);
        }
        AUTIL_LOG(INFO,
                  "use global JNIEnv, create and delete jobject elapsed: %ld",
                  autil::TimeUtility::currentTime() - beginTime);
    }

    {
        beginTime = autil::TimeUtility::currentTime();
        for (size_t i = 0; i < times; i++) {
            JNIEnv *localEnv = Jvm::env();
            jobject ts = localEnv->NewLocalRef(js);
            __asm__ __volatile__("" ::: "memory");
            env->DeleteLocalRef(ts);
        }
        AUTIL_LOG(INFO,
                  "use local JNIEnv, create and delete jobject elapsed: %ld",
                  autil::TimeUtility::currentTime() - beginTime);
    }

    {
        beginTime = autil::TimeUtility::currentTime();
        for (size_t i = 0; i < times; i++) {
            JNIEnv *localEnv = Jvm::checkedEnv();
            jobject ts = localEnv->NewLocalRef(js);
            __asm__ __volatile__("" ::: "memory");
            env->DeleteLocalRef(ts);
        }
        AUTIL_LOG(INFO,
                  "use local JNIEnv2, create and delete jobject elapsed: %ld",
                  autil::TimeUtility::currentTime() - beginTime);
    }

    {
        JavaVM *jvm;
        jint rc = env->GetJavaVM(&jvm);
        ASSERT_EQ(0, rc);
        ASSERT_NE(nullptr, jvm);
        beginTime = autil::TimeUtility::currentTime();
        for (size_t i = 0; i < times; i++) {
            JNIEnv *env;
            jvm->GetEnv((void **)&env, JNI_VERSION_1_8);
            jobject ts = env->NewLocalRef(js);
            __asm__ __volatile__("" ::: "memory");
            env->DeleteLocalRef(ts);
        }
        AUTIL_LOG(INFO,
                  "get JNIEnv from JavaVM without TLS elapsed: %ld",
                  autil::TimeUtility::currentTime() - beginTime);
    }
}

TEST_F(JvmTest, testJString) {
    JNIEnv *env = Jvm::env();
    ASSERT_NE(env, nullptr);

    std::string s("hello world");
    jstring js = env->NewStringUTF(s.c_str());
    jsize lenjs = env->GetStringUTFLength(js);
    ASSERT_EQ(s.size(), lenjs);

    const char *p = env->GetStringUTFChars(js, nullptr);
    std::string sj(p, lenjs);
    env->ReleaseStringUTFChars(js, p);
    env->DeleteLocalRef(js);
    ASSERT_EQ(s, sj);
}

} // namespace iquan
