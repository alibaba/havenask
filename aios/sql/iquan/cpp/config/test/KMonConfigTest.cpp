#include "iquan/config/KMonConfig.h"

#include <string>

#include "autil/Log.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/test/JsonTestUtil.h"
#include "iquan/common/IquanException.h"
#include "iquan/common/Status.h"
#include "iquan/common/Utils.h"
#include "unittest/unittest.h"

using namespace testing;

namespace iquan {

class KMonConfigTest : public TESTBASE {
private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(iquan, KMonConfigTest);

TEST_F(KMonConfigTest, testKMonConfig) {
    KMonConfig config;
    ASSERT_FALSE(config.isValid());

    config.serviceName = "test";
    ASSERT_TRUE(config.isValid());
    ASSERT_EQ("127.0.0.1:4141", config.flumeAddress);

    config.flumeAddress = "";
    ASSERT_FALSE(config.isValid());

    config.flumeAddress = "192.0.0.1:4141";
    KMonConfig config2 = config;
    ASSERT_EQ("192.0.0.1:4141", config.flumeAddress);
}

TEST_F(KMonConfigTest, testFromJson) {
    std::string expectStr = R"json({
"auto_recycle":
  false,
"flume_address":
  "127.1.1.1:4141",
"global_tags":
  "test",
"service_name":
  "test",
"tenant_name":
  "test"
})json";

    KMonConfig config;
    Status status = Utils::fromJson(config, expectStr);
    ASSERT_TRUE(status.ok()) << status.errorMessage();
    ASSERT_EQ("test", config.serviceName);
    ASSERT_EQ("test", config.tenantName);
    ASSERT_EQ("127.1.1.1:4141", config.flumeAddress);
    ASSERT_EQ(false, config.autoRecycle);
    ASSERT_EQ("test", config.globalTags);
}

TEST_F(KMonConfigTest, testToJson) {
    std::string expectStr = R"json({
"auto_recycle":
  true,
"flume_address":
  "127.0.0.1:4141",
"global_tags":
  "",
"service_name":
  "kmon.sql",
"tenant_name":
  "default"
})json";

    KMonConfig config;
    config.serviceName = "kmon.sql";
    std::string actualStr;
    Status status = Utils::toJson(config, actualStr, false);
    ASSERT_TRUE(status.ok()) << status.errorMessage();
    ASSERT_NO_FATAL_FAILURE(
        autil::legacy::JsonTestUtil::checkJsonStringEqual(expectStr, actualStr));
}

} // namespace iquan
