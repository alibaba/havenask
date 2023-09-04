#include "iquan/jni/wrapper/JClassLoader.h"

#include "iquan/jni/test/testlib/IquanTestBase.h"

namespace iquan {

class JClassLoaderTest : public IquanTestBase {};

TEST_F(JClassLoaderTest, testGetSystemClassLoader) {
    LocalRef<JClassLoader> classLoader = JClassLoader::getSystemClassLoader();
    ASSERT_TRUE(classLoader.get() != nullptr);

    std::string name = classLoader->toStdString();
    std::size_t pos = name.find("sun.misc.Launcher$AppClassLoader@");
    ASSERT_EQ(0, pos);
}

TEST_F(JClassLoaderTest, testGetExtClassLoader) {
    LocalRef<JClassLoader> classLoader = JClassLoader::getExtClassLoader();
    ASSERT_TRUE(classLoader.get() != nullptr);

    std::string name = classLoader->toStdString();
    std::size_t pos = name.find("sun.misc.Launcher$ExtClassLoader@");
    ASSERT_EQ(0, pos);
}

TEST_F(JClassLoaderTest, testGetParent) {
    LocalRef<JClassLoader> classLoader = JClassLoader::getSystemClassLoader();
    ASSERT_TRUE(classLoader.get() != nullptr);
    {
        std::string name = classLoader->toStdString();
        std::size_t pos = name.find("sun.misc.Launcher$AppClassLoader@");
        ASSERT_EQ(0, pos);
    }

    LocalRef<JClassLoader> parentClassLoader = classLoader->getParent();
    ASSERT_TRUE(parentClassLoader.get() != nullptr);
    {
        std::string name = parentClassLoader->toStdString();
        std::size_t pos = name.find("sun.misc.Launcher$ExtClassLoader@");
        ASSERT_EQ(0, pos);
    }
}

TEST_F(JClassLoaderTest, testLoadClass) {
    LocalRef<JClassLoader> classLoader = JClassLoader::getSystemClassLoader();
    ASSERT_TRUE(classLoader.get() != nullptr);

    LocalRef<JClass> clazz = classLoader->loadClass("java.lang.String");
    ASSERT_TRUE(clazz.get() != nullptr);
    ASSERT_EQ("class java.lang.String", clazz->toStdString());
}

} // namespace iquan
