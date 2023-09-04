#include "iquan/jni/wrapper/JURLClassLoader.h"

#include "iquan/jni/test/testlib/IquanTestBase.h"
#include "iquan/jni/wrapper/JClassLoader.h"
#include "iquan/jni/wrapper/JURL.h"

namespace iquan {

class JURLClassLoaderTest : public IquanTestBase {};

TEST_F(JURLClassLoaderTest, testNewJURLClassLoaderTest) {
    LocalRef<JURL> url = JURL::newInstance("file:test-path");
    ASSERT_TRUE(url.get() != nullptr);

    LocalRef<JObjectArrayT<JURL>> urlArray = JObjectArrayT<JURL>::newArray(1);
    ASSERT_TRUE(urlArray.get() != nullptr);
    urlArray->setElement(0, url);

    LocalRef<JClassLoader> extClassLoader = JClassLoader::getExtClassLoader();
    ASSERT_TRUE(extClassLoader.get() != nullptr);

    LocalRef<JURLClassLoader> urlClassLoader
        = JURLClassLoader::newInstance(urlArray, extClassLoader);
    ASSERT_TRUE(urlClassLoader.get() != nullptr);
    std::string result = urlClassLoader->toStdString();
    std::size_t pos = result.find("java.net.URLClassLoader@");
    ASSERT_EQ(0, pos);
}

} // namespace iquan
