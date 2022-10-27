#include "build_service/test/unittest.h"
#include "build_service/util/EnvUtil.h"

using namespace std;
using namespace testing;

namespace build_service {
namespace util {

class EnvUtilTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
};

void EnvUtilTest::setUp() {
}

void EnvUtilTest::tearDown() {
}

TEST_F(EnvUtilTest, testSimple) {
    const char* envName = "env_util_test_temp_env";
    uint32_t tmp = 0;
    ASSERT_FALSE(EnvUtil::getValueFromEnv(envName, tmp));
    setenv(envName, "12", true);
    ASSERT_TRUE(EnvUtil::getValueFromEnv(envName, tmp));
    ASSERT_EQ(12, tmp);
    unsetenv(envName);
    ASSERT_FALSE(EnvUtil::getValueFromEnv(envName, tmp));
    ASSERT_EQ(12, tmp);
    setenv(envName, "abc", true);
    ASSERT_FALSE(EnvUtil::getValueFromEnv(envName, tmp));
    unsetenv(envName);
}

}
}
