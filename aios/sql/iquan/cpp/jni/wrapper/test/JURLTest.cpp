#include "iquan/jni/wrapper/JURL.h"

#include "iquan/jni/test/testlib/IquanTestBase.h"

namespace iquan {

class JURLTest : public IquanTestBase {};

TEST_F(JURLTest, testNewURL) {
    LocalRef<JURL> url = JURL::newInstance("file:test-path");
    ASSERT_TRUE(url.get() != nullptr);

    std::string result = url->toStdString();
    ASSERT_EQ("file:test-path", result);
}

} // namespace iquan
